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
pub struct Basic {
    #[serde(alias = "usr")]
    #[serde(alias = "user")]
    pub username: String,
    #[serde(alias = "pwd")]
    #[serde(alias = "pass")]
    pub password: String,
    pub parameters: Value,
}

impl fmt::Debug for Basic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut obfuscate_username = self.username.clone();
        obfuscate_username.replace_range(
            0..(obfuscate_username.len() / 2),
            (0..(obfuscate_username.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );

        let mut obfuscate_password = self.password.clone();
        obfuscate_password.replace_range(
            0..(obfuscate_password.len() / 2),
            (0..(obfuscate_password.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );

        f.debug_struct("Basic")
            .field("username", &obfuscate_username)
            .field("password", &obfuscate_password)
            .field("parameters", &self.parameters)
            .finish()
    }
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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::authenticator::basic::Basic;
    ///
    /// let username = "my_username";
    /// let password = "my_password";
    ///
    /// let auth = Basic::new(username, password);
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
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{Connector, curl::Curl};
    /// use chewdata::document::json::Json;
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Box::new(Json::default());
    ///
    ///     let username = "my_username";
    ///     let password = "my_password";
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Basic(Basic::new(
    ///         username, password,
    ///     ))));
    ///     connector.method = Method::Get;
    ///     connector.path = format!("/basic-auth/{}/{}", username, password);
    ///     let datastream = connector.fetch(document).await.unwrap().unwrap();
    ///     let len = datastream.count().await;
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
            username.replace_mustache(parameters.clone());
        }
        if password.has_mustache() {
            password.replace_mustache(parameters);
        }

        let basic = base64::encode(format!("{}:{}", username, password));

        Ok(request_builder.header(headers::AUTHORIZATION, format!("Basic {}", basic)))
    }
    /// See [`Authenticator::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{authenticator::AuthenticatorType, curl::Curl, Connector};
    use crate::document::json::Json;
    use async_std::prelude::StreamExt;
    use http_types::Method;

    #[test]
    fn new() {
        let username = "my_username";
        let password = "my_password";
        let auth = Basic::new(username, password);
        assert_eq!(username, auth.username);
        assert_eq!(password, auth.password);
    }
    #[async_std::test]
    async fn authenticate() {
        let document = Box::new(Json::default());

        let username = "my_username";
        let password = "my_password";
        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.authenticator_type = Some(Box::new(AuthenticatorType::Basic(Basic::new(
            username, password,
        ))));
        connector.method = Method::Get;
        connector.path = format!("/basic-auth/{}/{}", username, password);
        let datastream = connector.fetch(document).await.unwrap().unwrap();
        let len = datastream.count().await;
        assert!(0 < len, "Should read one some bytes.");
    }
    #[async_std::test]
    async fn authenticate_fail() {
        let document = Box::new(Json::default());

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.authenticator_type = Some(Box::new(AuthenticatorType::Basic(Basic::new(
            "bad_username",
            "bad_password",
        ))));
        connector.method = Method::Get;
        connector.path = "/basic-auth/true_username/true_password".to_string();
        match connector.fetch(document).await {
            Ok(_) => assert!(false, "Should generate an error."),
            Err(_) => assert!(true),
        };
    }
    #[async_std::test]
    async fn authenticate_with_username_password_in_param() {
        let document = Box::new(Json::default());

        let mut connector = Curl::default();
        connector.endpoint = "http://localhost:8080".to_string();
        connector.authenticator_type = Some(Box::new(AuthenticatorType::Basic(Basic::new(
            "{{ username }}",
            "{{ password }}",
        ))));
        connector.method = Method::Get;
        connector.path = format!("/basic-auth/{}/{}", "my_username", "my_password");
        connector.parameters =
            serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#).unwrap();
        let datastream = connector.fetch(document).await.unwrap().unwrap();
        let len = datastream.count().await;
        assert!(0 < len, "Should read one some bytes.");
    }
}
