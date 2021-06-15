use super::Authenticator;
use crate::helper::mustache::Mustache;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};
use async_trait::async_trait;
use surf::{RequestBuilder, http::headers};

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

#[async_trait]
impl Authenticator for Basic {
    /// See [`Authenticator::authenticate`] for more details.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::{Connector, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let username = "my_username";
    ///     let password = "my_password";
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(AuthenticatorType::Basic(Basic::new(username, password)));
    ///     connector.method = Method::Get;
    ///     connector.path = format!("/basic-auth/{}/{}", username, password);
    ///     connector.fetch().await?;
    ///     let mut buffer = String::default();
    ///     let len = connector.read_to_string(&mut buffer).await?;
    ///     assert!(0 < len, "Should read one some bytes.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: failed the authentification
    /// ```
    /// use chewdata::connector::{Connector, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(AuthenticatorType::Basic(Basic::new("bad_username", "bad_password")));
    ///     connector.method = Method::Get;
    ///     connector.path = "/basic-auth/true_username/true_password".to_string();
    ///     match connector.fetch().await {
    ///         Ok(_) => assert!(false, "Should generate an error."),
    ///         Err(_) => assert!(true),
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Set username/password with parameters
    /// ```
    /// use chewdata::connector::{Connector, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic};
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let username = "{{ username }}";
    ///     let password = "{{ password }}";
    /// 
    ///     let mut parameters: Value = serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#)?;
    /// 
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(AuthenticatorType::Basic(Basic::new(username, password)));
    ///     connector.method = Method::Get;
    ///     connector.path = format!("/basic-auth/{}/{}", "my_username", "my_password");
    ///     connector.parameters = parameters;
    ///     connector.fetch().await?;
    ///     let mut buffer = String::default();
    ///     let len = connector.read_to_string(&mut buffer).await?;;
    ///     assert!(0 < len, "Should read one some bytes.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn authenticate(&mut self, request_builder: RequestBuilder) -> Result<RequestBuilder> {
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

        let basic = base64::encode(format!("{}:{}", username, password));

        Ok(request_builder.header(headers::AUTHORIZATION, format!("Basic {}", basic)))
    }
    /// See [`Authenticator::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
}
