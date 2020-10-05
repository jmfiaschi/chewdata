use super::Authenticate;
use curl::easy::{Easy, List};
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Bearer {
    pub token: String,
    pub base64: bool,
}

impl Default for Bearer {
    fn default() -> Self {
        Bearer {
            token: "".to_owned(),
            base64: false,
        }
    }
}

impl Bearer {
    /// Add authentification to a request and connect the system to a document protected by bearer token.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{Authenticator, bearer::Bearer};
    /// use chewdata::connector::Connect;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let token = "abcd1234";
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator = Some(Authenticator::Bearer(Bearer::new(token)));
    /// connector.method = Method::Get;
    /// connector.path = "/bearer".to_string();
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    /// # Example: failed the authentification
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{Authenticator, bearer::Bearer};
    /// use chewdata::connector::Connect;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let bad_token = "";
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator = Some(Authenticator::Bearer(Bearer::new(bad_token)));
    /// connector.method = Method::Get;
    /// connector.path = "/bearer".to_string();
    /// let mut buffer = String::default();
    /// match connector.read_to_string(&mut buffer) {
    ///     Ok(_) => assert!(false, "Should generate an error."),
    ///     Err(_) => assert!(true),
    /// };
    /// ```
    pub fn new(token: &str) -> Self {
        Bearer {
            token: token.to_string(),
            base64: false,
        }
    }
    pub fn base64(self) -> Self {
        Bearer {
            token: self.token,
            base64: true,
        }
    }
}

impl Authenticate for Bearer {
    fn add_authentication(&self, _client: &mut Easy, headers: &mut List) -> Result<()> {
        if let "" = self.token.as_ref() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Bearer authentification require a token.",
            ));
        }

        let mut token = self.token.clone();

        if self.base64 {
            token = base64::encode(token);
        }

        headers.append(format!("Authorization: Bearer {}", token).as_ref())?;

        Ok(())
    }
}
