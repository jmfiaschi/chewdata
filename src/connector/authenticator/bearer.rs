use super::Authenticator;
use crate::helper::mustache::Mustache;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};
use surf::{http::headers, RequestBuilder};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Bearer {
    pub token: String,
    pub is_base64: bool,
    pub parameters: Value,
}

impl fmt::Debug for Bearer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut obfuscate_token = self.token.clone();
        obfuscate_token.replace_range(
            0..(obfuscate_token.len() / 2),
            (0..(obfuscate_token.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );

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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::authenticator::bearer::Bearer;
    ///
    /// let token = "my_token";
    ///
    /// let auth = Bearer::new(token);
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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{Connector, curl::Curl};
    /// use chewdata::document::json::Json;
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///     let token = "abcd1234";
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Bearer(Bearer::new(token))));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     let datastream = connector.fetch(document).await.unwrap().unwrap();
    ///     let len = datastream.count().await;
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

#[cfg(test)]
mod tests {
    use async_std::prelude::StreamExt;
    use http_types::Method;
    use crate::document::json::Json;
    use crate::connector::{authenticator::AuthenticatorType, curl::Curl, Connector};
    use super::*;

    #[test]
    fn new() {
        let token = "my_token";
        let auth = Bearer::new(token);
        assert_eq!(token, auth.token);
        assert_eq!(false, auth.is_base64);
    }
    #[async_std::test]
    async fn authenticate() {
        let document = Box::new(Json::default());
        let token = "abcd1234";
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.authenticator_type =
            Some(Box::new(AuthenticatorType::Bearer(Bearer::new(token))));
        connector.method = Method::Get;
        connector.path = "/bearer".to_string();
        let datastream = connector.fetch(document).await.unwrap().unwrap();
        let len = datastream.count().await;
        assert!(0 < len, "Should read one some bytes.");
    }
    #[async_std::test]
    async fn authenticate_fail() {
        let document = Box::new(Json::default());

        let bad_token = "";
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.authenticator_type =
            Some(Box::new(AuthenticatorType::Bearer(Bearer::new(bad_token))));
        connector.method = Method::Get;
        connector.path = "/bearer".to_string();
        match connector.fetch(document).await {
            Ok(_) => assert!(false, "Should generate an error."),
            Err(_) => assert!(true),
        };
    }
    #[async_std::test]
    async fn authenticate_with_token_in_param() {
        let document = Box::new(Json::default());

        let token = "{{ token }}";
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.authenticator_type =
            Some(Box::new(AuthenticatorType::Bearer(Bearer::new(token))));
        connector.method = Method::Get;
        connector.path = "/bearer".to_string();
        connector.parameters = serde_json::from_str(r#"{"token":"my_token"}"#).unwrap();
        let datastream = connector.fetch(document).await.unwrap().unwrap();
        let len = datastream.count().await;
        assert!(0 < len, "Should read one some bytes.");
    }
}
