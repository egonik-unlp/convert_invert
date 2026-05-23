use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager};
use dotenvy::dotenv;
use std::env;

pub mod manager;
pub mod model;
pub mod schema;

pub type DbPool = r2d2::Pool<ConnectionManager<PgConnection>>;

pub fn init_pool() -> anyhow::Result<DbPool> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL")?;
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    r2d2::Pool::builder().build(manager).map_err(Into::into)
}

pub fn establish_connection() -> anyhow::Result<PgConnection> {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL")?;
    PgConnection::establish(&database_url).map_err(Into::into)
}
