use super::Authenticator;
use crate::helper::mustache::Mustache;
use async_trait::async_trait;
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
    /// use chewdata::connector::authenticator::{AuthenticatorType, bearer::Bearer};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let token = "my_token";
    /// 
    ///     let (auth_name, auth_value) = Bearer::new(token).authenticate(Value::Null).await.unwrap();
    /// 
    ///     assert_eq!(auth_name, "authorization".to_string().into_bytes());
    ///     assert_eq!(auth_value, format!("Bearer {}", token).as_bytes().to_vec());
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn authenticate(&mut self, parameters: Value) -> Result<(Vec<u8>, Vec<u8>)> {
        if let "" = self.token.as_ref() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Bearer authentification require a token.",
            ));
        }

        let mut token = self.token.clone();

        if token.has_mustache() {
            token.replace_mustache(parameters);
        }

        if self.is_base64 {
            token = base64::encode(token);
        }

        let bearer = base64::encode(token);

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

        let (auth_name, auth_value) = Bearer::new(token).authenticate(Value::Null).await.unwrap();

        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(auth_value, format!("Bearer {}", base64::encode(token)).as_bytes().to_vec());
    }
}
