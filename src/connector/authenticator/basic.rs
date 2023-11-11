//! Authenticate the request with basic auth.
//!
//! ### Configuration
//!
//!| key      | alias      | Description                                  | Default Value | Possible Values |
//!| -------- | ---------- | -------------------------------------------- | ------------- | --------------- |
//!| type     | -          | Required in order to use this authentication | `basic`       | `basic`         |
//!| username | user / usr | Username to use for the authentification     | `null`        | String          |
//!| password | pass / pwd | Password to use for the authentification     | `null`        | String          |
//!
//! ### Examples
//!
//!```json
//![
//!    {
//!        "type": "read",
//!        "connector":{
//!            "type": "curl",
//!            "endpoint": "{{ CURL_ENDPOINT }}",
//!            "path": "/get",
//!            "method": "get",
//!            "authenticator": {
//!                "type": "basic",
//!                "username": "{{ BASIC_USERNAME }}",
//!                "password": "{{ BASIC_PASSWORD }}",
//!            }
//!        },
//!    }
//!]
//!```
use super::Authenticator;
use crate::helper::mustache::Mustache;
use async_trait::async_trait;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};
use surf::http::headers;

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Basic {
    #[serde(alias = "usr")]
    #[serde(alias = "user")]
    pub username: String,
    #[serde(alias = "pwd")]
    #[serde(alias = "pass")]
    pub password: String,
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
            .finish()
    }
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
    /// ```
    pub fn new(username: &str, password: &str) -> Self {
        Basic {
            username: username.to_string(),
            password: password.to_string(),
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
    /// use chewdata::connector::authenticator::{AuthenticatorType, basic::Basic, Authenticator};
    /// use async_std::prelude::*;
    /// use std::io;
    /// use base64::Engine;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let username = "{{ username }}";
    ///     let password = "{{ password }}";
    ///     let parameters = serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#).unwrap();
    ///
    ///     let (auth_name, auth_value) = Basic::new(username, password)
    ///         .authenticate(&parameters)
    ///         .await
    ///         .unwrap();
    ///
    ///     assert_eq!(auth_name, "authorization".to_string().into_bytes());
    ///     assert_eq!(
    ///         auth_value,
    ///         format!(
    ///             "Basic {}",
    ///             base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", "my_username", "my_password"))
    ///         )
    ///         .as_bytes()
    ///         .to_vec()
    ///     );
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn authenticate(&mut self, parameters: &Value) -> Result<(Vec<u8>, Vec<u8>)> {
        if let ("", "") = (self.username.as_ref(), self.password.as_ref()) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Basic authentification require a username and a password.",
            ));
        }

        let mut username = self.username.clone();
        let mut password = self.password.clone();

        if username.has_mustache() {
            username.replace_mustache(parameters.clone());
        }
        if password.has_mustache() {
            password.replace_mustache(parameters.clone());
        }

        let basic =
            base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", username, password));

        Ok((
            headers::AUTHORIZATION.as_str().as_bytes().to_vec(),
            format!("Basic {}", basic).as_bytes().to_vec(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn authenticate() {
        let username = "my_username";
        let password = "my_password";

        let (auth_name, auth_value) = Basic::new(username, password)
            .authenticate(&Value::Null)
            .await
            .unwrap();

        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(
            auth_value,
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{}", username, password))
            )
            .as_bytes()
            .to_vec()
        );
    }
    #[async_std::test]
    async fn authenticate_with_username_password_in_param() {
        let username = "{{ username }}";
        let password = "{{ password }}";
        let parameters =
            serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#).unwrap();

        let (auth_name, auth_value) = Basic::new(username, password)
            .authenticate(&parameters)
            .await
            .unwrap();

        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(
            auth_value,
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{}", "my_username", "my_password"))
            )
            .as_bytes()
            .to_vec()
        );
    }
}
