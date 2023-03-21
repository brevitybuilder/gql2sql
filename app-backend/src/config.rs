use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub admin_database_url: String,

    pub gotrue_url: String,

    pub jwt_secret: String,

    pub max_db_connections: u32,

    pub backend_port: u16,

    pub service_key: String,

    pub user_database_url: String,
}
