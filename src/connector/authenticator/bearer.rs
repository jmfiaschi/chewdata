use super::Authenticator;
use crate::helper::mustache::Mustache;
use curl::easy::{Easy, List};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Bearer {
    pub token: String,
    pub is_base64: bool,
    pub parameters: Value,
}

impl Default for Bearer {
    fn default() -> Self {
        Bearer {
            token: "".to_owned(),
            is_base64: false,
            parameters: Value::Null,
        }
    }
}

impl Bearer {
    /// Get new bearer.
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::authenticator::bearer::Bearer;
    ///
    /// let token = "my_token";
    ///
    /// let mut auth = Bearer::new(token);
    ///
    /// assert_eq!(token, auth.token);
    /// assert_eq!(false, auth.is_base64);
    /// ```
    pub fn new(token: &str) -> Self {
        Bearer {
            token: token.to_string(),
            is_base64: false,
            ..Default::default()
        }
    }
}

impl Authenticator for Bearer {
    /// Add authentification to a request and connect the system to a document protected by bearer token.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use std::io::Read;
    ///
    /// let token = "abcd1234";
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator_type = Some(AuthenticatorType::Bearer(Bearer::new(token)));
    /// connector.method = Method::Get;
    /// connector.path = "/bearer".to_string();
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    /// # Example: failed the authentification
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use std::io::Read;
    ///
    /// let bad_token = "";
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator_type = Some(AuthenticatorType::Bearer(Bearer::new(bad_token)));
    /// connector.method = Method::Get;
    /// connector.path = "/bearer".to_string();
    /// let mut buffer = String::default();
    /// match connector.read_to_string(&mut buffer) {
    ///     Ok(_) => assert!(false, "Should generate an error."),
    ///     Err(_) => assert!(true),
    /// };
    /// ```
    /// # Example: Set token with parameters
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let token = "{{ token }}";
    ///
    /// let mut parameters: Value = serde_json::from_str(r#"{"token":"my_token"}"#).unwrap();
    ///
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator_type = Some(AuthenticatorType::Bearer(Bearer::new(token)));
    /// connector.method = Method::Get;
    /// connector.path = "/bearer".to_string();
    /// connector.parameters = parameters;
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    fn add_authentication(&mut self, _client: &mut Easy, headers: &mut List) -> Result<()> {
        if let "" = self.token.as_ref() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Bearer authentification require a token.",
            ));
        }

        let mut token = self.token.clone();
        let parameters = self.parameters.clone();

        if token.has_mustache() {
            token = token.replace_mustache(parameters);
        }

        if self.is_base64 {
            token = base64::encode(token);
        }

        headers.append(format!("Authorization: Bearer {}", token).as_ref())?;

        Ok(())
    }
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
}
