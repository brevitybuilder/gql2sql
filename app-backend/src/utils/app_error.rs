use axum::{
    response::{IntoResponse, Response},
    Json,
};
use http::StatusCode;

#[derive(Debug)]
pub enum AppError {
    ReqwestError(reqwest::Error),
    SQLxError(sqlx::Error),
    Anyhow(anyhow::Error),
    Validate(validator::ValidationErrors),
    Error(StatusCode, String),
    IoError(std::io::Error),
}

impl AppError {
    pub fn new(status_code: StatusCode, message: String) -> Result<Response, Self> {
        Err(Self::Error(status_code, message))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        println!("AppError: {:?}", self);
        match self {
            AppError::Anyhow(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(format!("Something went wrong: {}", e)),
            )
                .into_response(),
            AppError::Error(status_code, message) => (status_code, Json(message)).into_response(),
            AppError::SQLxError(e) => match e {
                sqlx::Error::Database(e) => match e.code() {
                    Some(e) => match e.clone().into_owned().as_str() {
                        "42P07" => (
                            StatusCode::BAD_REQUEST,
                            Json("Table already exists".to_string()),
                        )
                            .into_response(),
                        "42703" => (
                            StatusCode::BAD_REQUEST,
                            Json("Column already exists".to_string()),
                        )
                            .into_response(),
                        "42P16" => (
                            StatusCode::BAD_REQUEST,
                            Json("Index already exists".to_string()),
                        )
                            .into_response(),
                        "42P01" => (
                            StatusCode::BAD_REQUEST,
                            Json("Table does not exist".to_string()),
                        )
                            .into_response(),
                        _ => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(format!(
                                "Something went wrong with the database request: {}",
                                e
                            )),
                        )
                            .into_response(),
                    },
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(format!("Something went wrong: {}", e)),
                    )
                        .into_response(),
                },
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(format!("Something went wrong: {}", e)),
                )
                    .into_response(),
            },
            AppError::IoError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(format!("IO error: {}", e)),
            )
                .into_response(),
            AppError::Validate(e) => (
                StatusCode::BAD_REQUEST,
                Json(format!("Validation error: {}", e)),
            )
                .into_response(),
            AppError::ReqwestError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(format!("Something went wrong: {}", e)),
            )
                .into_response(),
            // AppError::BadRequest(e) => (StatusCode::BAD_REQUEST, Json(e)).into_response(),
        }
    }
}

impl From<validator::ValidationErrors> for AppError {
    fn from(err: validator::ValidationErrors) -> Self {
        Self::Validate(err)
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        Self::Anyhow(err)
    }
}

impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        Self::ReqwestError(err)
    }
}

impl From<sqlx::Error> for AppError {
    fn from(err: sqlx::Error) -> Self {
        Self::SQLxError(err)
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

pub type AppResponse = Result<Response, AppError>;
