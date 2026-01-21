#[cfg(test)]
mod tests {
    use crate::log_reader::CDCLogReaderBuilder;
    use std::collections::{HashMap, VecDeque};
    use std::convert::identity;
    use std::hash::Hash;
    use std::sync::Arc;
    use std::time::{self, Duration};

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use futures::future::RemoteHandle;
    use itertools::{Itertools, repeat_n};
    use rstest::rstest;
    use scylla::client::session::Session;
    use scylla::frame::response::result::ColumnType;
    use scylla::serialize::SerializationError;
    use scylla::serialize::value::SerializeValue;
    use scylla::serialize::writers::{CellWriter, WrittenCellProof};
    use scylla::statement::prepared::PreparedStatement;
    use scylla::value::CqlValue;
    use scylla_cdc_test_utils::{now, prepare_db, skip_if_not_supported};
    use scylla_proxy::{Reaction, ResponseReaction, ResponseRule, RunningProxy};
    use tokio::sync::Mutex;
    use tokio::sync::mpsc::Sender;
    use tracing::info;
    use tracing_test::traced_test;

    use crate::checkpoints::TableBackedCheckpointSaver;
    use crate::consumer::*;

    const SECOND_IN_MILLIS: u64 = 1_000;
    const SLEEP_INTERVAL: u64 = SECOND_IN_MILLIS / 10;
    const WINDOW_SIZE: u64 = SECOND_IN_MILLIS / 10 * 3;
    const SAFETY_INTERVAL: u64 = SECOND_IN_MILLIS / 10;

    type OperationsMap = Arc<Mutex<HashMap<Vec<PrimaryKeyValue>, VecDeque<Operation>>>>;

    // The driver's CqlValue cannot be used as HashMap key,
    // because it doesn't have the Eq trait.
    #[derive(Debug, Eq, PartialEq, Hash)]
    enum PrimaryKeyValue {
        // Name consistency with CqlValue from the driver is recommended.
        Int(i32),
        Text(String),
        List(Vec<PrimaryKeyValue>),
    }

    impl SerializeValue for PrimaryKeyValue {
        fn serialize<'b>(
            &self,
            typ: &ColumnType,
            writer: CellWriter<'b>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            self.to_cql().serialize(typ, writer)
        }
    }

    impl PrimaryKeyValue {
        pub fn from_cql(cql_val: &CqlValue) -> Option<PrimaryKeyValue> {
            match cql_val {
                CqlValue::Int(x) => Some(PrimaryKeyValue::Int(*x)),
                CqlValue::Text(s) => Some(PrimaryKeyValue::Text(s.clone())),
                CqlValue::List(v) => v
                    .iter()
                    .map(PrimaryKeyValue::from_cql)
                    .collect::<Option<Vec<PrimaryKeyValue>>>()
                    .map(PrimaryKeyValue::List),
                _ => None,
            }
        }

        pub fn to_cql(&self) -> CqlValue {
            match self {
                PrimaryKeyValue::Int(x) => CqlValue::Int(*x),
                PrimaryKeyValue::Text(s) => CqlValue::Text(s.clone()),
                PrimaryKeyValue::List(v) => {
                    CqlValue::List(v.iter().map(PrimaryKeyValue::to_cql).collect())
                }
            }
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    struct Operation {
        operation_type: OperationType,
        clustering_key: Option<i32>,
        value: Option<i32>,
    }

    impl Operation {
        fn new(
            operation_type: OperationType,
            clustering_key: Option<i32>,
            value: Option<i32>,
        ) -> Operation {
            Operation {
                operation_type,
                clustering_key,
                value,
            }
        }
    }

    struct TestConsumer {
        read_operations: OperationsMap,
    }

    #[async_trait]
    impl Consumer for TestConsumer {
        async fn consume_cdc(&mut self, mut data: CDCRow<'_>) -> Result<()> {
            let pk_val = {
                // Primary key columns have names pk1, pk2...
                let mut values = Vec::new();
                let mut i = 1;
                while data.column_exists(&format!("pk{i}")) {
                    let val = data.get_value(&format!("pk{i}")).as_ref().unwrap();
                    values.push(PrimaryKeyValue::from_cql(val).unwrap());
                    i += 1;
                }

                values
            };
            let op_type = data.operation.clone();
            let ck = match data.take_value("ck") {
                Some(CqlValue::Int(x)) => Some(x),
                None => None,
                Some(cql) => bail!("Unexpected ck type: {:?}", cql),
            };
            let val = data.take_value("v").map(|cql| cql.as_int().unwrap());

            self.read_operations
                .lock()
                .await
                .entry(pk_val)
                .or_insert_with(VecDeque::new)
                .push_back(Operation::new(op_type, ck, val));

            Ok(())
        }
    }

    struct TestConsumerFactory {
        read_operations: OperationsMap,
    }

    impl TestConsumerFactory {
        fn new(operations: OperationsMap) -> TestConsumerFactory {
            TestConsumerFactory {
                read_operations: operations,
            }
        }
    }

    #[async_trait]
    impl ConsumerFactory for TestConsumerFactory {
        async fn new_consumer(&self) -> Box<dyn Consumer> {
            Box::new(TestConsumer {
                read_operations: Arc::clone(&self.read_operations),
            })
        }
    }

    // Get queries to create, insert to and update a table.
    fn get_queries(table_name: &str, pk_type_names: Vec<&str>) -> (String, String, String) {
        let pk_definitions = pk_type_names
            .iter()
            .enumerate()
            .map(|(i, type_name)| format!("pk{} {}", i + 1, type_name))
            .join(", ");
        let primary_key_tuple = (1..pk_type_names.len() + 1)
            .map(|i| format!("pk{i}"))
            .join(", ");
        let binds = repeat_n('?', pk_type_names.len()).join(", ");
        let pk_conditions = (1..pk_type_names.len() + 1)
            .map(|i| format!("pk{i} = ?"))
            .join(" AND ");

        (
            format!(
                "CREATE TABLE {table_name} ({pk_definitions}, ck int, v int, primary key (({primary_key_tuple}), ck)) WITH cdc = {{'enabled' : true}}"
            ),
            format!("INSERT INTO {table_name} (v, {primary_key_tuple}, ck) VALUES ({binds}, ?, ?)"),
            format!("UPDATE {table_name} SET v = ? WHERE {pk_conditions} AND ck = ?"),
        )
    }

    struct Test {
        session: Arc<Session>,
        keyspace: String,
        performed_operations: HashMap<Vec<PrimaryKeyValue>, VecDeque<Operation>>,
        table_name: String,
        insert_query: PreparedStatement,
        update_query: PreparedStatement,
    }

    impl Test {
        async fn new(
            table_name: &str,
            pk_type_names: Vec<&str>,
            tablets_enabled: bool,
        ) -> Result<Test> {
            let (create_query, insert_query, update_query) = get_queries(table_name, pk_type_names);

            let (session, keyspace) = prepare_db(&[create_query], 1, tablets_enabled).await?;
            let insert_query = session.prepare(insert_query).await?;
            let update_query = session.prepare(update_query).await?;

            Ok(Test {
                session,
                keyspace,
                performed_operations: HashMap::new(),
                table_name: table_name.to_string(),
                insert_query,
                update_query,
            })
        }

        fn push_back(&mut self, pk: Vec<PrimaryKeyValue>, operation: Operation) {
            self.performed_operations
                .entry(pk)
                .or_default()
                .push_back(operation);
        }

        fn get_value_list(
            pk_vec: &[PrimaryKeyValue],
            ck: i32,
            v: Option<i32>,
        ) -> Vec<Option<CqlValue>> {
            let mut list: Vec<Option<CqlValue>> = vec![v.map(CqlValue::Int)];
            list.extend(pk_vec.iter().map(|x| Some(x.to_cql())));
            list.push(Some(CqlValue::Int(ck)));

            list
        }

        async fn insert(
            &mut self,
            pk_vec: Vec<PrimaryKeyValue>,
            ck: i32,
            v: Option<i32>,
        ) -> Result<()> {
            self.session
                .execute_unpaged(&self.insert_query, Test::get_value_list(&pk_vec, ck, v))
                .await?;
            let operation = Operation::new(OperationType::RowInsert, Some(ck), v);
            self.push_back(pk_vec, operation);

            Ok(())
        }

        async fn update(
            &mut self,
            pk_vec: Vec<PrimaryKeyValue>,
            ck: i32,
            v: Option<i32>,
        ) -> Result<()> {
            self.session
                .execute_unpaged(&self.update_query, Test::get_value_list(&pk_vec, ck, v))
                .await?;
            let operation = Operation::new(OperationType::RowUpdate, Some(ck), v);
            self.push_back(pk_vec, operation);

            Ok(())
        }

        async fn compare(&mut self, result: OperationsMap) -> bool {
            let mut results = result.lock().await.iter_mut().map(|(pk, actual_operations)| {
                let mut expected_operations = match self.performed_operations.remove(pk) {
                    Some(ops) => ops,
                    None => {
                        eprintln!("Unexpected primary key {pk:?}");
                        return false;
                    }
                };
                let mut i = 0;

                loop {
                    match (expected_operations.pop_front(), actual_operations.pop_front()) {
                        (Some(next_expected), Some(next_actual)) => {
                            i += 1;
                            if next_expected == next_actual {
                                continue;
                            }
                            eprintln!("Operation no. {i} not matching for primary key {pk:?}.");
                            eprintln!("\tExpected: {next_expected:?}, actual: {next_actual:?}");
                        },
                        (None, None) => return true,
                        (None, _) => eprintln!("Too many read operations for primary key {:?}. Operations left: {}", pk, actual_operations.len() + 1),
                        (_, None) => eprintln!("Too little read operations for primary key {:?}. Missing operations count: {}", pk, expected_operations.len() + 1),
                    }
                    return false;
                }
            }).collect::<Vec<_>>();

            for pk in self.performed_operations.keys() {
                eprintln!("Expected primary key {pk:?} not found");
                results.push(false);
            }
            results.into_iter().all(identity)
        }

        async fn test_cdc(mut self, start: chrono::Duration) -> Result<()> {
            let results = Arc::new(Mutex::new(HashMap::new()));
            let factory = Arc::new(TestConsumerFactory::new(Arc::clone(&results)));
            let end = now();

            let (_tester, handle) = CDCLogReaderBuilder::new()
                .session(Arc::clone(&self.session))
                .keyspace(self.keyspace.as_str())
                .table_name(self.table_name.as_str())
                .start_timestamp(start - chrono::Duration::seconds(2))
                .end_timestamp(end + chrono::Duration::seconds(2))
                .window_size(time::Duration::from_millis(WINDOW_SIZE))
                .safety_interval(time::Duration::from_millis(SAFETY_INTERVAL))
                .sleep_interval(time::Duration::from_millis(SLEEP_INTERVAL))
                .consumer_factory(factory)
                .build()
                .await
                .expect("Creating cdc log printer failed!");

            handle.await.unwrap();

            if !self.compare(results).await {
                panic!(
                    "{}",
                    format!("Test not passed for table {}.", self.table_name)
                );
            }

            Ok(())
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn e2e_test_small(#[case] tablets_enabled: bool) {
        let mut test =
            skip_if_not_supported!(Test::new("int_small_test", vec!["int"], tablets_enabled));
        let start = now();

        for i in 0..10 {
            for j in (3..6).rev() {
                test.insert(vec![PrimaryKeyValue::Int(i)], j, Some(i * j))
                    .await
                    .unwrap();
            }
        }

        for i in (0..10).rev() {
            for j in 3..6 {
                test.update(vec![PrimaryKeyValue::Int(i)], j, Some((i - j) * (i + j)))
                    .await
                    .unwrap();
            }
        }

        test.test_cdc(start).await.unwrap();
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn e2e_test_int_pk(#[case] tablets_enabled: bool) {
        let mut test = skip_if_not_supported!(Test::new("int_test", vec!["int"], tablets_enabled));
        let start = now();

        for i in 0..100 {
            for j in (300..400).rev() {
                test.insert(vec![PrimaryKeyValue::Int(i)], j, Some(i * j))
                    .await
                    .unwrap();
            }
        }

        for i in (0..100).rev() {
            for j in 300..400 {
                test.update(vec![PrimaryKeyValue::Int(i)], j, Some((i - j) * (i + j)))
                    .await
                    .unwrap();
            }
        }

        test.test_cdc(start).await.unwrap();
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn e2e_test_int_string_pk(#[case] tablets_enabled: bool) {
        let mut test = skip_if_not_supported!(Test::new(
            "int_string_test",
            vec!["int", "text"],
            tablets_enabled
        ));
        let strings = ["blep".to_string(), "nghu".to_string(), "pkeee".to_string()];
        let start = now();

        for i in 0..100 {
            for j in (300..400).rev() {
                test.insert(
                    vec![
                        PrimaryKeyValue::Int(i),
                        PrimaryKeyValue::Text(strings[(i % 3) as usize].clone()),
                    ],
                    j,
                    Some(i * j),
                )
                .await
                .unwrap();
            }
        }

        for i in (0..100).rev() {
            for j in 300..400 {
                test.update(
                    vec![
                        PrimaryKeyValue::Int(i),
                        PrimaryKeyValue::Text(strings[(i % 3) as usize].clone()),
                    ],
                    j,
                    Some((i - j) * (i + j)),
                )
                .await
                .unwrap();
            }
        }

        test.test_cdc(start).await.unwrap();
    }

    async fn create_reader_with_saving(
        test: &Test,
        factory: &Arc<TestConsumerFactory>,
        start: chrono::Duration,
        end: chrono::Duration,
    ) -> RemoteHandle<Result<()>> {
        let default_cp_saver = Arc::new(
            TableBackedCheckpointSaver::new(
                test.session.clone(),
                &test.keyspace,
                &format!("{}_checkpoints", test.table_name),
                300,
            )
            .await
            .unwrap(),
        );
        let (_tester, handle) = CDCLogReaderBuilder::new()
            .session(Arc::clone(&test.session))
            .keyspace(test.keyspace.as_str())
            .table_name(test.table_name.as_str())
            .start_timestamp(start)
            .end_timestamp(end)
            .window_size(time::Duration::from_millis(WINDOW_SIZE))
            .safety_interval(time::Duration::from_millis(SAFETY_INTERVAL))
            .sleep_interval(time::Duration::from_millis(SLEEP_INTERVAL))
            .consumer_factory(factory.clone())
            .should_save_progress(true)
            .should_load_progress(true)
            .pause_between_saves(time::Duration::from_millis(SLEEP_INTERVAL))
            .checkpoint_saver(default_cp_saver)
            .build()
            .await
            .expect("Creating cdc log printer failed!");

        handle
    }

    async fn insert_new_rows_for_saving_test(test: &mut Test, index: i32) {
        const INTERVAL_SIZE: i32 = 30;
        for i in INTERVAL_SIZE * index..INTERVAL_SIZE * (index + 1) {
            for j in (INTERVAL_SIZE * index..INTERVAL_SIZE * (index + 1)).rev() {
                test.insert(vec![PrimaryKeyValue::Int(i)], j, Some(i * j))
                    .await
                    .unwrap();
            }
        }
    }

    #[rstest]
    #[case::vnodes(false)]
    #[case::tablets(true)]
    #[tokio::test]
    async fn e2e_test_saving_progress_complex(#[case] tablets_enabled: bool) {
        const N: i32 = 5;
        let table_name = "test_saving_progress";
        let start = now();

        let mut test = skip_if_not_supported!(Test::new(table_name, vec!["int"], tablets_enabled));

        let results = Arc::new(Mutex::new(HashMap::new()));
        let factory = Arc::new(TestConsumerFactory::new(Arc::clone(&results)));

        for i in 0..N {
            insert_new_rows_for_saving_test(&mut test, i).await;
            let end = now();

            let handle = create_reader_with_saving(
                &test,
                &factory,
                start,
                end + chrono::Duration::seconds(1),
            )
            .await;

            handle.await.unwrap();
        }

        if !test.compare(results).await {
            panic!(
                "{}",
                format!("Test not passed for table {}.", test.table_name)
            );
        }
    }

    async fn generate_cdc_update(session: &Arc<Session>, keyspace: &str) {
        session
            .query_unpaged(
                format!("INSERT INTO {}.cdc_test_table (id) VALUES (?)", keyspace),
                (uuid::Uuid::new_v4(),),
            )
            .await
            .unwrap();
    }

    async fn init_db(session: &Arc<Session>, keyspace: &str) {
        // We need to disable tablets for this test. See https://github.com/scylladb/scylladb/issues/16317
        session
        .query_unpaged(
            format!("CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}", keyspace),
            ()
        )
        .await.unwrap();

        session
        .query_unpaged(
            format!("CREATE TABLE IF NOT EXISTS {}.cdc_test_table (id UUID PRIMARY KEY) WITH cdc = {{'enabled': true}}", keyspace),
            ()
        )
        .await.unwrap();
    }

    enum State {
        WaitingForFirst,
        Disabled,
        WaitingForReconnection,
    }

    #[derive(Clone, Copy)]
    enum DropReason {
        DropPackets,
        _DropConnection,
    }

    // A simple consumer that just prints the received CDC data.
    struct SimpleConsumer {
        proxy: Arc<Mutex<RunningProxy>>,
        state: Arc<Mutex<State>>,
        finisher: Sender<()>,
        reconnection: Sender<()>,
        drop_reason: DropReason,
    }

    #[async_trait]
    impl Consumer for SimpleConsumer {
        async fn consume_cdc(&mut self, row: CDCRow<'_>) -> Result<()> {
            info!("Consuming cdc row {}", row.operation);
            let mut state = self.state.lock().await;

            if let State::WaitingForFirst = *state {
                let mut lock = self.proxy.lock().await;

                info!("Transitioning to dropped state");
                lock.running_nodes.iter_mut().for_each(|node| {
                    node.change_response_rules(Some(vec![ResponseRule(
                        scylla_proxy::Condition::True,
                        match self.drop_reason {
                            DropReason::DropPackets => Reaction::drop_frame(),
                            DropReason::_DropConnection => {
                                Reaction::drop_connection_with_delay(Duration::from_secs(1))
                            }
                        },
                    )]))
                });

                *state = State::Disabled;
                drop(state);
                drop(lock);

                self.reconnection.send(()).await.unwrap();
                /* let lock_clone = self.proxy.clone();
                let state_clone = self.state.clone();
                let reconnection = self.reconnection.clone();
                tokio::task::spawn(async move {
                }); */
            } else if let State::WaitingForReconnection = *state {
                info!("Got update after reconnection.");
                self.finisher.send(()).await.unwrap();
            }

            Ok(())
        }
    }

    struct SimpleConsumerFactory {
        proxy: Arc<Mutex<RunningProxy>>,
        state: Arc<Mutex<State>>,
        finisher: Sender<()>,
        reconnection: Sender<()>,
        drop_reason: DropReason,
    }

    #[async_trait]
    impl ConsumerFactory for SimpleConsumerFactory {
        async fn new_consumer(&self) -> Box<dyn Consumer> {
            Box::new(SimpleConsumer {
                proxy: __self.proxy.clone(),
                state: __self.state.clone(),
                finisher: __self.finisher.clone(),
                reconnection: __self.reconnection.clone(),
                drop_reason: __self.drop_reason,
            })
        }
    }

    #[rstest]
    #[case::drop_packets(DropReason::DropPackets, "127.0.0.70:9042", "test_keyspace_1")]
    // TODO: Would require merging #144
    // #[case::drop_packets(DropReason::DropConnection, "127.0.0.71:9042", "test_keyspace_2")]
    #[tokio::test]
    #[traced_test]
    #[ntest::timeout(100_000)]
    async fn should_recover_from_dropped_packets(
        #[case] drop_reason: DropReason,
        #[case] proxy_address: &str,
        #[case] keyspace: &str,
    ) {
        // In case of test flakiness, increase the safety_interval and window_size
        // in the CDCLogReaderBuilder. The current times were enough for local testing,
        // but there may be conditions, where those times may be insufficient.
        use std::{net::SocketAddr, str::FromStr};

        use scylla::client::{
            execution_profile::ExecutionProfileBuilder, session_builder::SessionBuilder,
        };
        use scylla_proxy::{Node, Proxy, ShardAwareness};
        use tokio::sync::mpsc::channel;

        let db_address =
            std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        info!("Attempting to connect to ScyllaDB at {}", db_address);

        let node1_real_addr = SocketAddr::from_str(&db_address).unwrap();
        let node1_proxy_addr = SocketAddr::from_str(proxy_address).unwrap();
        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::QueryNode,
            None,
            None,
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let builder = SessionBuilder::new().known_node(proxy_address);

        let session: Arc<Session> = Arc::new(
            builder
                .default_execution_profile_handle(
                    ExecutionProfileBuilder::default()
                        .request_timeout(Some(Duration::from_millis(500)))
                        .build()
                        .into_handle(),
                )
                .keepalive_timeout(Duration::from_millis(1500))
                .build()
                .await
                .unwrap(),
        );

        let direct_session: Arc<Session> = Arc::new(
            SessionBuilder::new()
                .known_node(&db_address)
                .build()
                .await
                .unwrap(),
        );

        init_db(&direct_session, keyspace).await;

        info!("Session created successfully.");

        let state = Arc::new(Mutex::new(State::WaitingForFirst));
        let proxy = Arc::new(Mutex::new(running_proxy));
        let (finisher_tx, mut finisher_rx) = channel(1);
        let (reconnection_tx, mut reconnection_rx) = channel(1);

        let (mut reader, handle) = CDCLogReaderBuilder::new()
            .session(session.clone())
            .keyspace(keyspace)
            .table_name("cdc_test_table")
            .window_size(Duration::from_secs(20))
            .safety_interval(Duration::from_secs(5))
            .consumer_factory(Arc::new(SimpleConsumerFactory {
                proxy: proxy.clone(),
                state: state.clone(),
                finisher: finisher_tx,
                reconnection: reconnection_tx,
                drop_reason,
            }))
            .build()
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;
        info!("Sending row to trigger CDC...");
        generate_cdc_update(&direct_session, keyspace).await;

        reconnection_rx.recv().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        info!("Connection re-established, sending another update...");
        generate_cdc_update(&direct_session, keyspace).await;
        
        tokio::time::sleep(Duration::from_secs(10)).await;
        let mut state = state.lock().await;
        let mut proxy = proxy.lock().await;
        info!("Re-enabling connections");

        *state = State::WaitingForReconnection;
        proxy
            .running_nodes
            .iter_mut()
            .for_each(|node| node.change_response_rules(Some(vec![])));

        drop(state);
        drop(proxy);
        

        finisher_rx.recv().await;
        info!("Shutdown signal received.");
        reader.stop();
        handle.await.unwrap();

        info!("CDC stream stopped.");
    }
}
