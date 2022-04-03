use super::Authenticator;
use crate::helper::mustache::Mustache;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, io::{Error, ErrorKind, Result}};
use async_trait::async_trait;
use surf::{RequestBuilder, http::headers};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Bearer {
    pub token: String,
    pub is_base64: bool,
    pub parameters: Value,
}

impl fmt::Debug for Bearer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut obfuscate_token = self
            .token
            .clone();
        obfuscate_token.replace_range(0..(obfuscate_token.len()/2), (0..(obfuscate_token.len()/2)).map(|_| "#").collect::<String>().as_str());

        f.debug_struct("Bearer")
            .field("token", &obfuscate_token)
            .field("is_base64", &self.is_base64)
            .field("parameters", &self.parameters)
            .finish()
    }
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

#[async_trait]
impl Authenticator for Bearer {
    /// See [`Authenticator::authenticate`] for more details.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::{Connector, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let token = "abcd1234";
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Bearer(Bearer::new(token))));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
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
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let bad_token = "";
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Bearer(Bearer::new(bad_token))));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     match connector.fetch().await {
    ///         Ok(_) => assert!(false, "Should generate an error."),
    ///         Err(_) => assert!(true),
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Set token with parameters
    /// ```
    /// use chewdata::connector::{Connector, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let token = "{{ token }}";
    /// 
    ///     let mut parameters: Value = serde_json::from_str(r#"{"token":"my_token"}"#)?;
    /// 
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Bearer(Bearer::new(token))));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     connector.parameters = parameters;
    ///     connector.fetch().await?;
    ///     let mut buffer = String::default();
    ///     let len = connector.read_to_string(&mut buffer).await?;
    ///     assert!(0 < len, "Should read one some bytes.");
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn authenticate(&mut self, request_builder: RequestBuilder) -> Result<RequestBuilder> {
        if let "" = self.token.as_ref() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Bearer authentification require a token.",
            ));
        }

        let mut token = self.token.clone();
        let parameters = self.parameters.clone();

        if token.has_mustache() {
            token.replace_mustache(parameters);
        }

        if self.is_base64 {
            token = base64::encode(token);
        }

        let bearer = base64::encode(token);

        Ok(request_builder.header(headers::AUTHORIZATION, format!("Bearer {}", bearer)))
    }
    /// See [`Authenticator::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
}
