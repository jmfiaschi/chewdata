//! Authenticate the request with bearer token.
//!
//! ### Configuration
//!
//! | key        | alias | Description                                                             | Default Value | Possible Values          |
//! | ---------- | ----- | ----------------------------------------------------------------------- | ------------- | ------------------------ |
//! | type       | -     | Required in order to use this authentication                            | `bearer`      | `bearer`                 |
//! | token      | -     | The bearer token                                                        | `null`        | String                   |
//! | is_base64  | -     | Specify if the bearer token is encoded in base64                        | `true`       | `false` / `true`         |
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
//!             }
//!         },
//!     }
//! ]
//! ```
use crate::helper::string::{DisplayOnlyForDebugging, Obfuscate};

use super::Authenticator;
use async_trait::async_trait;
use base64::Engine;
use http::header;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Bearer {
    pub token: String,
    pub is_base64: bool,
}

impl fmt::Debug for Bearer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bearer")
            .field(
                "token",
                &self.token.to_obfuscate().display_only_for_debugging(),
            )
            .field("is_base64", &self.is_base64)
            .finish()
    }
}

impl Default for Bearer {
    fn default() -> Self {
        Bearer {
            token: "".to_owned(),
            is_base64: true,
        }
    }
}

impl Bearer {
    /// Get new bearer.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::authenticator::bearer::Bearer;
    ///
    /// let token = "my_token";
    ///
    /// let auth = Bearer::new(token);
    /// ```
    pub fn new(token: &str) -> Self {
        Bearer {
            token: token.to_string(),
            is_base64: true,
        }
    }
}

#[async_trait]
impl Authenticator for Bearer {
    /// See [`Authenticator::authenticate`] for more details.
    ///
    /// # Examples
    ///
    /// ```
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer, Authenticator};
    /// use smol::prelude::*;
    /// use std::io;
    /// use serde_json::Value;
    ///
    /// use macro_rules_attribute::apply;
    /// use smol_macros::main;
    ///
    /// #[apply(main!)]
    /// async fn main() -> io::Result<()> {
    ///     let token = "my_token";
    ///
    ///     let (auth_name, auth_value) = Bearer::new(token).authenticate().await?;
    ///
    ///     assert_eq!(auth_name, "authorization".to_string().into_bytes());
    ///     assert_eq!(auth_value, format!("Bearer {}", token).as_bytes().to_vec());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn authenticate(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        if self.token.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Bearer authentification require a token",
            ));
        }

        let mut token = self.token.clone();

        if !self.is_base64 {
            token = base64::engine::general_purpose::STANDARD.encode(token);
        }

        Ok((
            header::AUTHORIZATION.to_string().into_bytes(),
            format!("Bearer {}", token).into_bytes(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use macro_rules_attribute::apply;
    use smol_macros::test;

    #[apply(test!)]
    async fn authenticate_without_base64() {
        let token = "my_token_not_in_base64";

        let mut bearer = Bearer::new(token);
        bearer.is_base64 = false;

        let (auth_name, auth_value) = bearer.authenticate().await.unwrap();

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
    #[apply(test!)]
    async fn authenticate_with_base64() {
        let token = "my_token_in_base64";

        let mut bearer = Bearer::new(token);
        bearer.is_base64 = true;

        let (auth_name, auth_value) = bearer.authenticate().await.unwrap();

        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(auth_value, format!("Bearer {}", token).as_bytes().to_vec());
    }
}
