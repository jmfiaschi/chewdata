pub mod basic;
pub mod bearer;
pub mod jwt;

use async_trait::async_trait;
use basic::Basic;
use bearer::Bearer;
use jwt::Jwt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Result;

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
    async fn authenticate(&mut self, parameters: Value) -> Result<(Vec<u8>, Vec<u8>)>;
}
