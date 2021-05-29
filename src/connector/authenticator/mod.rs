pub mod basic;
pub mod bearer;
pub mod jwt;

use basic::Basic;
use bearer::Bearer;
use jwt::Jwt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Result;
use async_trait::async_trait;
use http::request::Builder;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum AuthenticatorType {
    #[serde(rename = "basic")]
    Basic(Basic),
    #[serde(rename = "bearer")]
    Bearer(Bearer),
    #[serde(rename = "jwt")]
    Jwt(Jwt),
}

impl AuthenticatorType {
    pub fn authenticator(&self) -> &dyn Authenticator {
        match self {
            AuthenticatorType::Basic(authenticator) => authenticator,
            AuthenticatorType::Bearer(authenticator) => authenticator,
            AuthenticatorType::Jwt(authenticator) => authenticator,
        }
    }
    pub fn authenticator_mut(&mut self) -> &mut dyn Authenticator {
        match self {
            AuthenticatorType::Basic(authenticator) => authenticator,
            AuthenticatorType::Bearer(authenticator) => authenticator,
            AuthenticatorType::Jwt(authenticator) => authenticator,
        }
    }
}

#[async_trait]
pub trait Authenticator: Sync + Send {
    async fn add_authentication(&mut self, request_builder: Builder) -> Result<Builder>;
    fn set_parameters(&mut self, parameters: Value);
}
