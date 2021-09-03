// use rsdb::DriverManager;
use rsdb_postgres;
use rsdb_postgres::PostgresDriver;
use rsdb::{Driver, Value};
use rsdb::Result;
#[tokio::main]
async fn main()->Result<()>{
    let driver= PostgresDriver::new();
    let connection = driver.connect("host=localhost user=postgres password=postgres dbname=dellstore").await?;
    let stmt1=connection.prepare("SELECT * FROM categories where categoryname like $1").await?;
    let stmt2=connection.prepare("SELECT count(*) FROM customers where firstname like $1").await?;
    let mut result1 = stmt1.execute_query(&[Value::String("%e%".to_owned())]).await?;
    let mut result2 = stmt2.execute_query(&[Value::String("%UO%".to_owned())]).await?;
    while result1.next().await {
        let res1 = result1.get_string(1)?;
        let res0 = result1.get_i32(0)?;
        println!("{:?}-{:?}",res0,res1)
    };
    println!("=========================");
    while result2.next().await {
        let res1 = "Count";// result2.get_string(1)?;
        let res0 = result2.get_i64(0)?;
        println!("{:?}-{:?}",res0,res1)
    };
    Ok(())
    // let conn = DriverManager::get_connection("hello").await.unwrap();
    // conn.create_statement().await.execute_query("Hello World").await;
}