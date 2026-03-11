use actix_web::http::StatusCode;
use actix_web::{HttpResponse, web};
use serde_json::Value;

use crate::models::{ApiErrorResponse, AppState};

pub fn error_response(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    details: Option<Value>,
) -> HttpResponse {
    HttpResponse::build(status).json(ApiErrorResponse {
        code: code.to_string(),
        message: message.into(),
        details,
    })
}

pub fn bad_request(
    state: &web::Data<AppState>,
    code: &str,
    message: impl Into<String>,
    details: Option<Value>,
) -> HttpResponse {
    state.metrics.inc_bad_request();
    error_response(StatusCode::BAD_REQUEST, code, message, details)
}

pub fn internal_error(
    code: &str,
    message: impl Into<String>,
    details: Option<Value>,
) -> HttpResponse {
    error_response(StatusCode::INTERNAL_SERVER_ERROR, code, message, details)
}
