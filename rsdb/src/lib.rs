use async_trait::async_trait;
#[derive(Debug)]
pub enum Error {
    General(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    UInt32(u32),
    String(String),
    //TODO add other types
}

#[async_trait]
pub trait Driver: Sync + Send {
    /// Create a connection to the database. Note that connections are intended to be used
    /// in a single thread since most database connections are not thread-safe
    async fn connect(&self, url: &str) -> Result<Box<dyn Connection>>;
}

/// Represents a connection to a database
#[async_trait]
pub trait Connection {
    /// Create a statement for execution
    async fn create(&self, sql: &str) -> Result<Box<dyn Statement + '_>>;

    /// Create a prepared statement for execution
    async fn prepare(&self, sql: &str) -> Result<Box<dyn Statement + '_>>;
}

/// Represents an executable statement
#[async_trait]
pub trait Statement {
    /// Execute a query that is expected to return a result set, such as a `SELECT` statement
    async fn execute_query(&self, params: &[Value]) -> Result<Box<dyn ResultSet + '_>>;

    /// Execute a query that is expected to update some rows.
    async fn execute_update(&self, params: &[Value]) -> Result<u64>;
}

/// Result set from executing a query against a statement
#[async_trait]
pub trait ResultSet {
    /// get meta data about this result set
    async fn meta_data(&self) -> Result<Box<dyn ResultSetMetaData>>;

    /// Move the cursor to the next available row if one exists and return true if it does
    async fn next(&mut self) -> bool;

    fn get_i8(&self, i: u64) -> Result<i8>;
    fn get_i16(&self, i: u64) -> Result<i16>;
    fn get_i32(&self, i: u64) -> Result<i32>;
    fn get_i64(&self, i: u64) -> Result<i64>;
    fn get_f32(&self, i: u64) -> Result<f32>;
    fn get_f64(&self, i: u64) -> Result<f64>;
    fn get_string(&self, i: u64) -> Result<String>;
    fn get_bytes(&self, i: u64) -> Result<Vec<u8>>;
}

/// Meta data for result set
pub trait ResultSetMetaData {
    fn num_columns(&self) -> u64;
    fn column_name(&self, i: u64) -> String;
    fn column_type(&self, i: u64) -> DataType;
}

/// RDBC Data Types
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Byte,
    Char,
    Short,
    Integer,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Datetime,
    Utf8,
    Binary,
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Column {
            name: name.to_owned(),
            data_type,
        }
    }
}

impl ResultSetMetaData for Vec<Column> {
    fn num_columns(&self) -> u64 {
        self.len() as u64
    }

    fn column_name(&self, i: u64) -> String {
        self[i as usize].name.clone()
    }

    fn column_type(&self, i: u64) -> DataType {
        self[i as usize].data_type
    }
}