use std::{net::SocketAddr, sync::Arc};

use axum::{
    error_handling::HandleErrorLayer,
    http::{
        header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
        Method, StatusCode,
    },
    BoxError, Router,
};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{
    filter::{EnvFilter, LevelFilter},
    fmt,
    prelude::*,
};

use crate::{config::Config, routes};

#[derive(Clone)]
pub struct ApiContext {
    pub config: Arc<Config>,
    pub admin_db: PgPool,
    pub user_db: PgPool,
}

pub async fn run() {
    // load standard env variables
    let config = envy::from_env::<Config>().unwrap();

    // creates a postgres connection pool
    let admin_db = PgPoolOptions::new()
        .max_connections(config.max_db_connections)
        .connect(&config.admin_database_url)
        .await
        .expect("can't connect to admin database");

    let user_db = PgPoolOptions::new()
        .max_connections(config.max_db_connections)
        .connect(&config.user_database_url)
        .await
        .expect("can't connect to user database");

    // This embeds database migrations in the application binary so we can ensure the database
    // is migrated correctly on startup
    sqlx::migrate!()
        .run(&admin_db)
        .await
        .expect("can't run migrations");

    // loads tracing filter from env variable
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::ERROR.into())
        .from_env_lossy();

    // initializes tracing
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();

    // initializes cors
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_headers([AUTHORIZATION, ACCEPT, CONTENT_TYPE]);

    // creates a socket address to expose the api
    let addr = SocketAddr::from(([0, 0, 0, 0], config.backend_port));

    // loads the api context to be used throughout the applicaiton
    let context = ApiContext {
        config: Arc::new(config),
        admin_db,
        user_db,
    };

    // creates the api router and applies the middlewares
    let api_router = Router::new().merge(routes::router(context)).layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http())
            .layer(cors)
            .layer(HandleErrorLayer::new(|_: BoxError| async {
                StatusCode::REQUEST_TIMEOUT
            }))
            .timeout(std::time::Duration::from_secs(30)),
    );

    println!("All configured, starting server at {}", addr);
    // starts the server
    axum::Server::bind(&addr)
        .serve(api_router.into_make_service())
        .await
        .unwrap();
}
