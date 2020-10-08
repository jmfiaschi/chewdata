use super::Authenticator;
use crate::helper::mustache::Mustache;
use curl::easy::{Easy, List};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Basic {
    #[serde(alias = "usr")]
    #[serde(alias = "user")]
    pub username: String,
    #[serde(alias = "pwd")]
    #[serde(alias = "pass")]
    pub password: String,
    pub parameters: Value,
}

impl Default for Basic {
    fn default() -> Self {
        Basic {
            username: "".to_owned(),
            password: "".to_owned(),
            parameters: Value::Null,
        }
    }
}

impl Basic {
    /// Get new authentification
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::authenticator::basic::Basic;
    ///
    /// let username = "my_username";
    /// let password = "my_password";
    ///
    /// let mut auth = Basic::new(username, password);
    ///
    /// assert_eq!(username, auth.username);
    /// assert_eq!(password, auth.password);
    /// ```
    pub fn new(username: &str, password: &str) -> Self {
        Basic {
            username: username.to_string(),
            password: password.to_string(),
            ..Default::default()
        }
    }
}

impl Authenticator for Basic {
    /// Add authentification to a request and connect the system to a document protected by basic auth.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic};
    /// use std::io::Read;
    ///
    /// let username = "my_username";
    /// let password = "my_password";
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator_type = Some(AuthenticatorType::Basic(Basic::new(username, password)));
    /// connector.method = Method::Get;
    /// connector.path = format!("/basic-auth/{}/{}", username, password);
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    /// # Example: failed the authentification
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic};
    /// use std::io::Read;
    ///
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator_type = Some(AuthenticatorType::Basic(Basic::new("bad_username", "bad_password")));
    /// connector.method = Method::Get;
    /// connector.path = "/basic-auth/true_username/true_password".to_string();
    /// let mut buffer = String::default();
    /// match connector.read_to_string(&mut buffer) {
    ///     Ok(_) => assert!(false, "Should generate an error."),
    ///     Err(_) => assert!(true),
    /// };
    /// ```
    /// # Example: Set username/password with parameters
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic};
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let username = "{{ username }}";
    /// let password = "{{ password }}";
    ///
    /// let mut parameters: Value = serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#).unwrap();
    ///
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator_type = Some(AuthenticatorType::Basic(Basic::new(username, password)));
    /// connector.method = Method::Get;
    /// connector.path = format!("/basic-auth/{}/{}", "my_username", "my_password");
    /// connector.parameters = parameters;
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    fn add_authentication(&mut self, client: &mut Easy, _headers: &mut List) -> Result<()> {
        if let ("", "") = (self.username.as_ref(), self.password.as_ref()) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Basic authentification require a username and a password.",
            ));
        }

        let mut username = self.username.clone();
        let mut password = self.password.clone();
        let parameters = self.parameters.clone();

        if username.has_mustache() {
            username = username.replace_mustache(parameters.clone());
        }
        if password.has_mustache() {
            password = password.replace_mustache(parameters);
        }

        client.username(username.as_str())?;
        client.password(password.as_str())?;

        Ok(())
    }
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
}
