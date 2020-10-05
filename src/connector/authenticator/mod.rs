pub mod basic;
pub mod bearer;
pub mod jwt;

use basic::Basic;
use bearer::Bearer;
use curl::easy::{Easy, List};
use jwt::Jwt;
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum Authenticator {
    #[serde(rename = "basic")]
    Basic(Basic),
    #[serde(rename = "bearer")]
    Bearer(Bearer),
    #[serde(rename = "jwt")]
    Jwt(Jwt),
}

impl Authenticator {
    pub fn get(&self) -> Box<&dyn Authenticate> {
        match self {
            Authenticator::Basic(authenticator) => Box::new(authenticator),
            Authenticator::Bearer(authenticator) => Box::new(authenticator),
            Authenticator::Jwt(authenticator) => Box::new(authenticator),
        }
    }
}

pub trait Authenticate {
    fn add_authentication(&self, client: &mut Easy, headers: &mut List) -> Result<()>;
}
