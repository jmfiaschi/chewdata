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
use crate::helper::string::{DisplayOnlyForDebugging, Obfuscate};

use super::Authenticator;
use async_trait::async_trait;
use base64::Engine;
use serde::{Deserialize, Serialize};
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

impl Default for Basic {
    fn default() -> Self {
        Basic {
            username: "".to_owned(),
            password: "".to_owned(),
        }
    }
}

impl fmt::Debug for Basic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Basic")
            .field("username", &self.username)
            .field(
                "password",
                &self
                    .password
                    .to_owned()
                    .to_obfuscate()
                    .display_only_for_debugging(),
            )
            .finish()
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
    /// let auth = Basic::new("my_username", "my_password");
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
    ///     let username = "my_username";
    ///     let password = "my_password";
    ///     let token_expected = "Basic bXlfdXNlcm5hbWU6bXlfcGFzc3dvcmQ=";
    ///
    ///     let (auth_name, auth_value) = Basic::new(username, password).authenticate().await?;
    ///     assert_eq!(auth_name, "authorization".to_string().into_bytes());
    ///     assert_eq!(token_expected.as_bytes(), auth_value);
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn authenticate(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        if let ("", "") = (self.username.as_ref(), self.password.as_ref()) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Basic authentification require a username and a password",
            ));
        }

        let basic = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", self.username, self.password));

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
        let token_expected = "Basic bXlfdXNlcm5hbWU6bXlfcGFzc3dvcmQ=";

        let (auth_name, auth_value) = Basic::new(username, password).authenticate().await.unwrap();
        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(token_expected.as_bytes(), auth_value);
    }
}
