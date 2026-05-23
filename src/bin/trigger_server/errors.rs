use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use serde_json::json;
use std::fmt;

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Unauthorized,
    NotFound(&'static str),
    Internal(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::BadRequest(message) => write!(formatter, "{message}"),
            ApiError::Unauthorized => write!(formatter, "Unauthorized"),
            ApiError::NotFound(message) => write!(formatter, "{message}"),
            ApiError::Internal(message) => write!(formatter, "{message}"),
        }
    }
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let body = json!({
            "error": self.to_string(),
            "code": match self {
                ApiError::BadRequest(_) => "bad_request",
                ApiError::Unauthorized => "unauthorized",
                ApiError::NotFound(_) => "not_found",
                ApiError::Internal(_) => "internal",
            },
        });
        HttpResponse::build(self.status_code()).json(body)
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        ApiError::Internal(error.to_string())
    }
}

impl From<diesel::result::Error> for ApiError {
    fn from(error: diesel::result::Error) -> Self {
        ApiError::Internal(error.to_string())
    }
}

impl From<diesel::r2d2::PoolError> for ApiError {
    fn from(error: diesel::r2d2::PoolError) -> Self {
        ApiError::Internal(error.to_string())
    }
}

impl From<redis::RedisError> for ApiError {
    fn from(error: redis::RedisError) -> Self {
        ApiError::Internal(error.to_string())
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
