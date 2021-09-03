use async_trait::async_trait;
use tokio_postgres::{NoTls, Client, Row, RowStream};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Word};
use sqlparser::dialect::keywords::Keyword;
use tokio_postgres::types::{BorrowToSql};
use futures_util::StreamExt;
use std::pin::Pin;
pub struct PostgresDriver {}
impl PostgresDriver {
    pub fn new() -> Self {
        PostgresDriver {}
    }
}

#[async_trait]
impl rsdb::Driver for PostgresDriver {
    async fn connect(&self, url: &str) -> rsdb::Result<Box<dyn rsdb::Connection>> {
        let (c, connection) = tokio_postgres::connect(url,NoTls).await.map_err(to_rsdb_err)?;//postgres::Connection::connect(url, TlsMode::None).map_err(to_rsdb_err)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        Ok(Box::new(PConnection::new(c)))
    }
}
struct PConnection {
    conn: Client,
}
impl PConnection {
    pub fn new(conn: Client) -> Self {
        Self { conn }
    }
}
#[async_trait]
impl rsdb::Connection for PConnection {
    async fn create(&self, sql: &str) -> rsdb::Result<Box<dyn rsdb::Statement + '_>> {
        self.prepare(sql).await
    }

    async fn prepare(&self, sql: &str) -> rsdb::Result<Box<dyn rsdb::Statement + '_>> {
        // translate SQL, mapping ? into $1 style bound param placeholder
        let dialect = PostgreSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut i = 0;
        let tokens: Vec<Token> = tokens
            .iter()
            .map(|t| match t {
                Token::Char(c) if *c == '?' => {
                    i += 1;
                    Token::Word(Word {
                        value: format!("${}", i),
                        quote_style: None,
                        keyword: Keyword::NONE,
                    })
                }
                _ => t.clone(),
            })
            .collect();
        let sql = tokens
            .iter()
            .map(|t| format!("{}", t))
            .collect::<Vec<String>>()
            .join("");

        Ok(Box::new(PStatement {
            conn: &self.conn,
            sql,
        }))
    }
}
struct PStatement<'a> {
    conn: &'a Client,
    sql: String,
}
#[async_trait]
impl<'a> rsdb::Statement for PStatement<'a> {
    async fn execute_query(
        &self,
        params: &[rsdb::Value],
    ) -> rsdb::Result<Box<dyn rsdb::ResultSet + '_>> {

        let rows = self
            .conn
            .query_raw(self.sql.as_str(), params.iter().map(|d|{
                match d {
                    rsdb::Value::String(s)=>s.borrow_to_sql(),
                    rsdb::Value::Int32(i)=>i.borrow_to_sql(),
                    rsdb::Value::UInt32(n)=>n.borrow_to_sql()
                }
            })).await
            .map_err(to_rsdb_err)?;
        Ok(Box::new(PResultSet { current_row:Option::None,rows:Box::pin(rows) }))
    }

    async fn execute_update(&self, params: &[rsdb::Value]) -> rsdb::Result<u64> {
        self.conn
            .execute_raw(self.sql.as_str(), params.iter().map(|d|{
                match d {
                    rsdb::Value::String(s)=>s.borrow_to_sql(),
                    rsdb::Value::Int32(i)=>i.borrow_to_sql(),
                    rsdb::Value::UInt32(n)=>n.borrow_to_sql()
                }
            })).await
            .map_err(to_rsdb_err)
    }
}

struct PResultSet {
    // meta: Vec<Column>,
    // i: usize,
    current_row : Option<Row>,
    rows: Pin<Box<RowStream>>,
}
macro_rules! impl_resultset_fns {
    ($($fn: ident -> $ty: ty),*) => {
        $(
            fn $fn(&self, i: u64) -> rsdb::Result<$ty> {
                if let Some(row)=&self.current_row{
                    Ok(row.get(i as usize))
                } else {
                    Err(rsdb::Error::General("Something went wrong".to_owned()))
                }
            }
        )*
    }
}
#[async_trait]
impl rsdb::ResultSet for PResultSet {
    async fn meta_data(&self) -> rsdb::Result<Box<dyn rsdb::ResultSetMetaData>> {
       Err(rsdb::Error::General("No Metadata".to_owned()))
    }

    async fn next(&mut self) -> bool {
        if let Some(Ok(row))=self.rows.next().await {
            row.get()
            self.current_row = Option::Some(row);
            true
        } else {
            false
        }
    }


    impl_resultset_fns! {
        get_i8 -> i8,
        get_i16 -> i16,
        get_i32 -> i32,
        get_i64 -> i64,
        get_f32 -> f32,
        get_f64 -> f64,
        get_string -> String,
        get_bytes -> Vec<u8>
    }
}



/// Convert a Postgres error into an rsdb error
fn to_rsdb_err(e: tokio_postgres::error::Error) -> rsdb::Error {
    rsdb::Error::General(format!("{:?}", e))
}


// fn to_postgres_value(values: &[rsdb::Value]) -> Vec<Box<(dyn tokio_postgres::types::ToSql+Sync)>> {
//     values
//         .iter()
//         .map(|v| match v {
//             rsdb::Value::String(s) => Box::new(s.clone()) as Box<(dyn tokio_postgres::types::ToSql+Sync)>,
//             rsdb::Value::Int32(n) => Box::new(*n) as Box<(dyn tokio_postgres::types::ToSql+Sync)>,
//             rsdb::Value::UInt32(n) => Box::new(*n) as Box<(dyn tokio_postgres::types::ToSql+Sync)>,
//             //TODO all types
//         })
//         .collect()
// }
// fn to_rsdb_type(ty: &Type) -> rsdb::DataType {
//     match ty.name() {
//         "" => rsdb::DataType::Bool,
//         //TODO all types
//         _ => rsdb::DataType::Utf8,
//     }
// }
