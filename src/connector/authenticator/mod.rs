pub mod basic;
pub mod bearer;
pub mod jwt;

use basic::Basic;
use bearer::Bearer;
use curl::easy::{Easy, List};
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

pub trait Authenticator {
    fn add_authentication(&mut self, client: &mut Easy, headers: &mut List) -> Result<()>;
    fn set_parameters(&mut self, parameters: Value);
}
