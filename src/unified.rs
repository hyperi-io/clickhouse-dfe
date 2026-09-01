// Project:   clickhouse-dfe
// File:      src/unified.rs
// Purpose:   One client dispatching over HTTP and TCP transports
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! One client over both transports. Reads land in [`Columns`] whichever
//! transport answered.
//!
//! [`Columns::get`] reads what [`FromColumn`] covers: `String`, `Vec<u8>`,
//! `u64`, `u8`, `i64`, `bool`, `Option<String>`; anything else errors on both.
//! `LowCardinality(_)` is left wrapped so it fails on HTTP as it already does
//! on TCP, and `JSON` reads on TCP alone -- upstream's Native reader rejects
//! the declared type outright.

use clickhouse::Client;
use clickhouse::native::decode::{Decode, ValueReader};
use clickhouse::native::{Block, Column, DataTypeNode};

use crate::error::Result;
use crate::native::{DecodedBlock, DecodedColumn, FromColumn};
use crate::tcp::TcpClient;

/// Sent on the HTTP arm to match the [`TcpClient`] default.
const JSON_AS_STRING: &str = "output_format_native_write_json_as_string";

/// Which wire protocol a [`UnifiedClient`] answers on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Transport {
    /// Upstream's HTTP client.
    Http,
    /// This crate's native TCP client.
    Tcp,
}

/// A ClickHouse client over either transport, cloned as cheaply as the inner
/// client. Configure that inner client with its own builder.
#[derive(Clone)]
#[non_exhaustive]
pub enum UnifiedClient {
    /// Speaks HTTP through upstream's client.
    Http(Client),
    /// Speaks the native protocol through this crate's pooled TCP client.
    Tcp(TcpClient),
}

impl UnifiedClient {
    /// Which transport this client answers on.
    #[must_use]
    pub fn transport(&self) -> Transport {
        match self {
            Self::Http(_) => Transport::Http,
            Self::Tcp(_) => Transport::Tcp,
        }
    }

    /// The inner HTTP client, for the operations only HTTP has -- notably
    /// `insert_formatted_with`, since the native protocol accepts no format
    /// but Native. `None` on the TCP arm.
    #[must_use]
    pub fn as_http(&self) -> Option<&Client> {
        match self {
            Self::Http(client) => Some(client),
            Self::Tcp(_) => None,
        }
    }

    /// The inner TCP client, for the operations only the native protocol has.
    /// `None` on the HTTP arm.
    #[must_use]
    pub fn as_tcp(&self) -> Option<&TcpClient> {
        match self {
            Self::Tcp(client) => Some(client),
            Self::Http(_) => None,
        }
    }

    /// Run a statement that returns no rows. `sql` goes over the wire verbatim
    /// -- no `?` binding, no `?fields`.
    ///
    /// # Errors
    ///
    /// Whatever the transport returns: a connection failure, or the server's
    /// own rejection of the statement.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        match self {
            Self::Http(client) => Ok(client.query_raw(sql).execute().await?),
            Self::Tcp(client) => client.query(sql).execute().await,
        }
    }

    /// Round-trip the server.
    ///
    /// # Errors
    ///
    /// A transport failure, or a credential the server rejects.
    pub async fn ping(&self) -> Result<()> {
        match self {
            // HTTP has no ping frame.
            Self::Http(client) => Ok(client.query_raw("SELECT 1").execute().await?),
            Self::Tcp(client) => client.ping().await,
        }
    }

    /// Run `sql` -- verbatim, as for [`Self::execute`] -- and collect the whole
    /// result set column-wise.
    ///
    /// # Errors
    ///
    /// A server rejection, or a column the block decoder does not carry.
    pub async fn fetch_columns(&self, sql: &str) -> Result<Columns> {
        let blocks = match self {
            Self::Http(client) => {
                let mut cursor = client
                    .query_raw(sql)
                    .with_setting(JSON_AS_STRING, "1")
                    .fetch_native()?;
                let mut blocks = Vec::new();
                while let Some(block) = cursor.next().await? {
                    blocks.push(decoded_block(&block)?);
                }
                blocks
            }
            Self::Tcp(client) => client.query(sql).fetch_blocks().await?,
        };
        Ok(Columns { blocks })
    }

    /// Open a dynamic insert, schema from `cache` else `system.columns`. TCP
    /// resolves it here because `DynamicInsert::tcp` needs it up front.
    ///
    /// # Errors
    ///
    /// TCP only: [`crate::Error::Dynamic`] if the schema query fails or the
    /// table has no columns. Both are retriable once the cache is cold.
    #[cfg(feature = "dynamic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "dynamic")))]
    pub async fn dynamic_insert(
        &self,
        database: &str,
        table: &str,
        cache: std::sync::Arc<crate::dynamic::DynamicSchemaCache>,
    ) -> Result<crate::dynamic::DynamicInsert> {
        use crate::dynamic::DynamicInsert;
        use crate::dynamic::schema::{schema_from_system_columns, system_columns_sql};

        // Two arms, one of which returns: `if let` would have to name the
        // other arm's binding a second time to reach the TCP client.
        #[allow(clippy::single_match_else)]
        let client = match self {
            Self::Http(client) => {
                return Ok(DynamicInsert::http(client.clone(), database, table, cache));
            }
            Self::Tcp(client) => client,
        };

        let full = format!("{database}.{table}");
        let schema = if let Some(cached) = cache.get(&full) {
            cached
        } else {
            let columns = self
                .fetch_columns(&system_columns_sql(database, table))
                .await?;
            let rows = columns
                .get::<String>("name")?
                .into_iter()
                .zip(columns.get::<String>("col_type")?)
                .zip(columns.get::<String>("default_kind")?)
                .map(|((name, col_type), kind)| (name, col_type, kind));
            let fetched = schema_from_system_columns(full.clone(), rows)?;
            cache.insert(&full, std::sync::Arc::clone(&fetched));
            fetched
        };
        Ok(DynamicInsert::tcp(client.clone(), database, table, schema))
    }
}

/// A whole result set, column-oriented. Blocks are held as the server sent
/// them and flattened on read.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Columns {
    blocks: Vec<DecodedBlock>,
}

impl Columns {
    /// Every value of column `name`, in server order, across every block.
    ///
    /// # Errors
    ///
    /// [`crate::Error::SchemaMismatch`] if a block that declares columns does
    /// not declare `name`, or if the column does not read as `T`.
    pub fn get<T: FromColumn>(&self, name: &str) -> Result<Vec<T>> {
        let mut values = Vec::with_capacity(self.rows());
        for block in &self.blocks {
            values.extend(block.column_as::<T>(name)?);
        }
        Ok(values)
    }

    /// Rows across every block; schema-only and empty blocks count nothing.
    ///
    /// A block's declared count is saturated into `usize`, which only bites on
    /// a 32-bit target that could not have held the block anyway.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.blocks
            .iter()
            .map(|b| usize::try_from(b.num_rows).unwrap_or(usize::MAX))
            .sum()
    }

    /// Whether the result set carries no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows() == 0
    }
}

/// A `String` cell, borrowed verbatim. Upstream's own impls go through `&str`,
/// which rejects the non-UTF-8 bytes ClickHouse's `String` permits.
struct RawBytes<'a>(&'a [u8]);

impl<'a> Decode<'a> for RawBytes<'a> {
    fn compatible(data_type: &DataTypeNode) -> bool {
        matches!(data_type, DataTypeNode::String)
    }

    fn decode(
        reader: &mut ValueReader<'a>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(reader.native_bytes()))
    }
}

fn collect<'a, T: Decode<'a>>(column: &'a Column) -> Result<Vec<T>> {
    let values: std::result::Result<Vec<T>, clickhouse::error::Error> =
        column.iter::<T>()?.collect();
    Ok(values?)
}

fn decoded_block(block: &Block) -> Result<DecodedBlock> {
    let mut schema = Vec::with_capacity(block.columns().len());
    let mut columns = Vec::with_capacity(block.columns().len());
    for column in block.columns() {
        // `Column::name()` panics on a non-UTF-8 name; a lossy key still works.
        schema.push((
            String::from_utf8_lossy(column.name_bytes()).into_owned(),
            column.data_type().to_string(),
        ));
        columns.push(decoded_column(column)?);
    }
    Ok(DecodedBlock {
        columns,
        schema,
        num_rows: block.num_rows() as u64,
    })
}

/// Map onto the [`DecodedColumn`] the TCP reader produces for the same wire
/// type; anything else stays `Unsupported` so a read fails the same way.
fn decoded_column(column: &Column) -> Result<DecodedColumn> {
    // `SimpleAggregateFunction(f, T)` is wire-identical to `T` and the TCP
    // reader unwraps it too. `LowCardinality(_)` is deliberately left wrapped.
    let mut data_type = column.data_type();
    while let DataTypeNode::SimpleAggregateFunction(_, inner) = data_type {
        data_type = inner;
    }

    Ok(match data_type {
        DataTypeNode::String => DecodedColumn::String(
            collect::<RawBytes<'_>>(column)?
                .into_iter()
                .map(|cell| cell.0.to_vec())
                .collect(),
        ),
        DataTypeNode::UInt8 => DecodedColumn::UInt8(collect(column)?),
        DataTypeNode::UInt64 => DecodedColumn::UInt64(collect(column)?),
        DataTypeNode::Int64 => DecodedColumn::Int64(collect(column)?),
        // The TCP reader folds `Bool` onto `UInt8` as well.
        DataTypeNode::Bool => {
            DecodedColumn::UInt8(collect::<bool>(column)?.into_iter().map(u8::from).collect())
        }
        DataTypeNode::Nullable(inner) if matches!(**inner, DataTypeNode::String) => {
            let cells = collect::<Option<RawBytes<'_>>>(column)?;
            DecodedColumn::Nullable {
                mask: cells.iter().map(|c| u8::from(c.is_none())).collect(),
                child: Box::new(DecodedColumn::String(
                    cells
                        .into_iter()
                        .map(|c| c.map_or_else(Vec::new, |cell| cell.0.to_vec()))
                        .collect(),
                )),
            }
        }
        other => DecodedColumn::Unsupported(other.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use clickhouse::test::{Mock, handlers};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    use super::*;
    use crate::native::io::{ClickHouseBytesWrite, ClickHouseRead, ClickHouseWrite};
    use crate::tcp::mock::{serve_one_handshake, write_schema_block, write_string_payload_block};
    use crate::tcp::protocol::ServerPacketId;

    const NAMES: [&str; 3] = ["id", "tag", "doc"];
    const TYPES: [&str; 3] = ["UInt64", "String", "JSON"];

    /// Answer the handshake, then run `script` on the pool's one connection.
    async fn scripted<F, Fut>(script: F) -> (UnifiedClient, JoinHandle<TcpStream>)
    where
        F: FnOnce(TcpStream) -> Fut + Send + 'static,
        Fut: Future<Output = TcpStream> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            serve_one_handshake(&mut sock).await;
            script(sock).await
        });
        (UnifiedClient::Tcp(TcpClient::new(addr)), server)
    }

    async fn serve_two_string_columns(mut sock: TcpStream) -> TcpStream {
        write_schema_block(&mut sock, &[("name", "String"), ("col_type", "String")]).await;
        write_string_payload_block(&mut sock, &[("name", &NAMES), ("col_type", &TYPES)]).await;
        end_of_stream(&mut sock).await;
        sock
    }

    /// Same, plus the zero-column block a real server appends to every result.
    async fn serve_two_string_columns_then_empty(mut sock: TcpStream) -> TcpStream {
        write_schema_block(&mut sock, &[("name", "String"), ("col_type", "String")]).await;
        write_string_payload_block(&mut sock, &[("name", &NAMES), ("col_type", &TYPES)]).await;
        write_schema_block(&mut sock, &[]).await;
        end_of_stream(&mut sock).await;
        sock
    }

    async fn end_of_stream(sock: &mut TcpStream) {
        sock.write_var_uint(ServerPacketId::EndOfStream as u64)
            .await
            .unwrap();
        sock.flush().await.unwrap();
    }

    /// The same two columns as a `format=Native` HTTP body.
    fn native_body() -> Vec<u8> {
        let mut out = Vec::new();
        out.put_var_uint(2);
        out.put_var_uint(NAMES.len() as u64);
        for (name, values) in [("name", NAMES), ("col_type", TYPES)] {
            out.put_string(name);
            out.put_string("String");
            for value in values {
                out.put_string(value);
            }
        }
        out
    }

    /// One RowBinary `u8` per row is one byte, so the body is these bytes.
    fn serve_native_body(mock: &Mock) {
        mock.add(handlers::provide::<u8>(native_body()));
    }

    #[test]
    fn transport_names_the_variant() {
        assert_eq!(
            UnifiedClient::Http(Client::default()).transport(),
            Transport::Http
        );
        assert_eq!(
            UnifiedClient::Tcp(TcpClient::new("127.0.0.1:9000")).transport(),
            Transport::Tcp
        );
    }

    #[tokio::test]
    async fn tcp_execute_runs_a_statement() {
        let (client, server) = scripted(|mut sock: TcpStream| async move {
            end_of_stream(&mut sock).await;
            sock
        })
        .await;

        client
            .execute("CREATE TABLE t (a UInt8) ENGINE = Memory")
            .await
            .expect("the server answered EndOfStream");

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_ping_round_trips() {
        let (client, server) = scripted(|mut sock: TcpStream| async move {
            let _ping_packet_id = sock.read_var_uint().await.unwrap();
            sock.write_var_uint(ServerPacketId::Pong as u64)
                .await
                .unwrap();
            sock.flush().await.unwrap();
            sock
        })
        .await;

        client.ping().await.expect("the server answered Pong");

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_fetch_columns_reads_by_name() {
        let (client, server) = scripted(serve_two_string_columns).await;

        let columns = client.fetch_columns("SELECT name, col_type").await.unwrap();

        assert_eq!(columns.rows(), 3);
        assert!(!columns.is_empty());
        assert_eq!(columns.get::<String>("name").unwrap(), NAMES);
        assert_eq!(columns.get::<String>("col_type").unwrap(), TYPES);

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn http_execute_sends_the_sql_verbatim() {
        let mock = Mock::new();
        let recorded = mock.add(handlers::record_ddl());
        let client = UnifiedClient::Http(Client::default().with_url(mock.url()));

        client.execute("SELECT position('a?b', '?')").await.unwrap();

        assert_eq!(recorded.query().await, "SELECT position('a?b', '?')");
    }

    #[tokio::test]
    async fn http_ping_sends_select_1() {
        let mock = Mock::new();
        let recorded = mock.add(handlers::record_ddl());
        let client = UnifiedClient::Http(Client::default().with_url(mock.url()));

        client.ping().await.unwrap();

        assert_eq!(recorded.query().await, "SELECT 1");
    }

    /// The point of the adapter: one Native payload, two transports, one answer.
    #[tokio::test]
    async fn both_transports_read_the_same_payload_identically() {
        let mock = Mock::new();
        serve_native_body(&mock);
        let http = UnifiedClient::Http(Client::default().with_url(mock.url()));
        let (tcp, server) = scripted(serve_two_string_columns).await;

        let over_http = http.fetch_columns("SELECT name, col_type").await.unwrap();
        let over_tcp = tcp.fetch_columns("SELECT name, col_type").await.unwrap();

        assert_eq!(over_http.rows(), over_tcp.rows());
        assert_eq!(
            over_http.get::<String>("name").unwrap(),
            over_tcp.get::<String>("name").unwrap()
        );
        assert_eq!(
            over_http.get::<Vec<u8>>("col_type").unwrap(),
            over_tcp.get::<Vec<u8>>("col_type").unwrap()
        );

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn an_undeclared_column_is_an_error_on_both_transports() {
        let mock = Mock::new();
        serve_native_body(&mock);
        let http = UnifiedClient::Http(Client::default().with_url(mock.url()));
        let (tcp, server) = scripted(serve_two_string_columns).await;

        let over_http = http.fetch_columns("SELECT name, col_type").await.unwrap();
        let over_tcp = tcp.fetch_columns("SELECT name, col_type").await.unwrap();

        assert!(over_http.get::<String>("missing").is_err());
        assert!(over_tcp.get::<String>("missing").is_err());
        assert!(over_http.get::<u64>("name").is_err());
        assert!(over_tcp.get::<u64>("name").is_err());

        let _sock = server.await.unwrap();
    }

    #[tokio::test]
    async fn a_trailing_empty_block_reads_as_no_values() {
        let (client, server) = scripted(serve_two_string_columns_then_empty).await;

        let columns = client.fetch_columns("SELECT name, col_type").await.unwrap();

        assert_eq!(columns.rows(), 3);
        assert_eq!(columns.get::<String>("name").unwrap(), NAMES);

        let _sock = server.await.unwrap();
    }

    /// The server splits a result set into blocks of its own choosing, so a
    /// read has to concatenate them in the order they arrived.
    #[tokio::test]
    async fn get_flattens_values_across_blocks_in_server_order() {
        let (client, server) = scripted(|mut sock: TcpStream| async move {
            write_schema_block(&mut sock, &[("name", "String")]).await;
            write_string_payload_block(&mut sock, &[("name", &["a", "b"])]).await;
            write_string_payload_block(&mut sock, &[("name", &["c"])]).await;
            write_string_payload_block(&mut sock, &[("name", &["d", "e"])]).await;
            end_of_stream(&mut sock).await;
            sock
        })
        .await;

        let columns = client.fetch_columns("SELECT name").await.unwrap();

        assert_eq!(columns.rows(), 5);
        assert_eq!(
            columns.get::<String>("name").unwrap(),
            ["a", "b", "c", "d", "e"]
        );

        let _sock = server.await.unwrap();
    }

    /// A ping is only useful if a server rejection reaches the caller rather
    /// than reading as a healthy round trip.
    #[tokio::test]
    async fn ping_surfaces_a_server_rejection() {
        let mock = Mock::new();
        // 60 = UNKNOWN_TABLE, returned the way the server returns one.
        mock.add(handlers::exception(60));
        let client = UnifiedClient::Http(Client::default().with_url(mock.url()));

        let err = client.ping().await.expect_err("the mock rejects");

        assert!(
            matches!(err, crate::Error::BadResponse(_)),
            "a rejection must not read as success: {err:?}"
        );
    }

    /// The transport-specific escape hatches: each arm hands back its own
    /// client and nothing else.
    #[test]
    fn as_http_and_as_tcp_expose_only_their_own_arm() {
        let http = UnifiedClient::Http(Client::default());
        assert!(http.as_http().is_some());
        assert!(http.as_tcp().is_none());

        let tcp = UnifiedClient::Tcp(TcpClient::new("127.0.0.1:9000"));
        assert!(tcp.as_tcp().is_some());
        assert!(tcp.as_http().is_none());
    }

    #[cfg(feature = "dynamic")]
    async fn serve_system_columns(mut sock: TcpStream) -> TcpStream {
        let header = [
            ("name", "String"),
            ("col_type", "String"),
            ("default_kind", "String"),
        ];
        write_schema_block(&mut sock, &header).await;
        write_string_payload_block(
            &mut sock,
            &[
                ("name", &["id", "tag"]),
                ("col_type", &["UInt64", "String"]),
                ("default_kind", &["", "DEFAULT"]),
            ],
        )
        .await;
        end_of_stream(&mut sock).await;
        sock
    }

    #[cfg(feature = "dynamic")]
    #[tokio::test]
    async fn tcp_dynamic_insert_reads_the_schema_once_then_caches_it() {
        use std::time::Duration;

        let (client, server) = scripted(serve_system_columns).await;
        let cache = crate::dynamic::DynamicSchemaCache::new(Duration::from_secs(300));

        let insert = client
            .dynamic_insert("dfe", "events", cache.clone())
            .await
            .expect("the mock served system.columns");

        let schema = insert.schema().expect("tcp resolves the schema up front");
        assert_eq!(schema.table, "dfe.events");
        assert_eq!(schema.len(), 2);
        assert!(schema.column("id").is_some());
        assert_eq!(schema.optional_columns().count(), 1);

        // The mock is scripted for one query, so a second fetch finds nothing.
        let cached = client
            .dynamic_insert("dfe", "events", cache)
            .await
            .expect("second call is served from the cache");
        assert_eq!(cached.schema().expect("cached schema").len(), 2);

        let _sock = server.await.unwrap();
    }
}
