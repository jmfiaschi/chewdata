use super::Authenticate;
use curl::easy::{Easy, List};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Basic {
    #[serde(alias = "usr")]
    #[serde(alias = "user")]
    username: String,
    #[serde(alias = "pwd")]
    #[serde(alias = "pass")]
    password: String,
}

impl Default for Basic {
    fn default() -> Self {
        Basic {
            username: "".to_owned(),
            password: "".to_owned(),
        }
    }
}

impl Basic {
    /// Add authentification to a request and connect the system to a document protected by basic auth.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{Authenticator, basic::Basic};
    /// use chewdata::connector::Connect;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let username = "my_username";
    /// let password = "my_password";
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator = Some(Authenticator::Basic(Basic::new(username, password)));
    /// connector.method = Method::Get;
    /// connector.path = format!("/basic-auth/{}/{}", username, password);
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    /// # Example: failed the authentification
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{Authenticator, basic::Basic};
    /// use chewdata::connector::Connect;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator = Some(Authenticator::Basic(Basic::new("bad_username", "bad_password")));
    /// connector.method = Method::Get;
    /// connector.path = "/basic-auth/true_username/true_password".to_string();
    /// let mut buffer = String::default();
    /// match connector.read_to_string(&mut buffer) {
    ///     Ok(_) => assert!(false, "Should generate an error."),
    ///     Err(_) => assert!(true),
    /// };
    /// ```
    pub fn new(username: &str, password: &str) -> Self {
        Basic {
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

impl Authenticate for Basic {
    fn add_authentication(&self, client: &mut Easy, _headers: &mut List) -> Result<()> {
        if let ("", "") = (self.username.as_ref(), self.password.as_ref()) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Basic authentification require a username and a password.",
            ));
        }

        client.username(self.username.as_ref())?;
        client.password(self.password.as_ref())?;

        Ok(())
    }
}
