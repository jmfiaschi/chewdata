//! Authenticate the request with bearer token.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                             | Default Value | Possible Values          |
//! | ---------- | ----- | ----------------------------------------------------------------------- | ------------- | ------------------------ |
//! | type       | -     | Required in order to use this authentication                            | `bearer`      | `bearer`                 |
//! | token      | -     | The bearer token                                                       | `null`        | String                   |
//! | is_base64  | -     | Specify if the bearer token is encoded in base64                        | `false`       | `false` / `true`         |
//! | parameters | -     | Use to replace the token with dynamic value in input from the connector | `null`        | List of Key/Value string |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "write",
//!         "connector":{
//!             "type": "curl",
//!             "endpoint": "{{ CURL_ENDPOINT }}",
//!             "path": "/post",
//!             "method": "post",
//!             "authenticator": {
//!                 "type": "bearer",
//!                 "token": "{{ token }}",
//!                 "is_base64": false,
//!                 "parameters": {
//!                     "token": "my_token"
//!                 }
//!             }
//!         },
//!     }
//! ]
//! ```
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
pub struct Bearer {
    pub token: String,
    pub is_base64: bool,
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
            .finish()
    }
}

impl Default for Bearer {
    fn default() -> Self {
        Bearer {
            token: "".to_owned(),
            is_base64: false,
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
    /// ```
    pub fn new(token: &str) -> Self {
        Bearer {
            token: token.to_string(),
            is_base64: false,
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
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer, Authenticator};
    /// use async_std::prelude::*;
    /// use std::io;
    /// use serde_json::Value;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let token = "my_token";
    ///
    ///     let (auth_name, auth_value) = Bearer::new(token).authenticate(&Value::Null).await.unwrap();
    ///
    ///     assert_eq!(auth_name, "authorization".to_string().into_bytes());
    ///     assert_eq!(auth_value, format!("Bearer {}", token).as_bytes().to_vec());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn authenticate(&mut self, parameters: &Value) -> Result<(Vec<u8>, Vec<u8>)> {
        if self.token.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Bearer authentification require a token.",
            ));
        }

        let mut token = self.token.clone();

        if token.has_mustache() {
            token.replace_mustache(parameters.clone());
        }

        if self.is_base64 {
            token = base64::engine::general_purpose::STANDARD.encode(token);
        }

        let bearer = base64::engine::general_purpose::STANDARD.encode(token);

        Ok((
            headers::AUTHORIZATION.to_string().into_bytes(),
            format!("Bearer {}", bearer).into_bytes(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    async fn authenticate() {
        let token = "my_token";

        let (auth_name, auth_value) = Bearer::new(token).authenticate(&Value::Null).await.unwrap();

        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(
            auth_value,
            format!(
                "Bearer {}",
                base64::engine::general_purpose::STANDARD.encode(token)
            )
            .as_bytes()
            .to_vec()
        );
    }
}
