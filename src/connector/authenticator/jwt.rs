//! Authenticate the request with Java Web Token (JWT).
//!
//! ### Configuration
//!
//! | key               | alias | Description                                                          | Default Value | Possible Values                                                                            |
//! | ----------------- | ----- | -------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------ |
//! | type              | -     | Required in order to use this authentication                         | `jwt`         | `jwt`                                                                                      |
//! | algorithm         | algo  | The algorithm used to build the signing_type                         | `HS256`       | String                                                                                     |
//! | refresh_connector | refresh | The connector used to refresh the token                            | `null`        | See [Connectors](#connectors)                                                              |
//! | jwk               | -     | The Json web key used to sign                                        | `null`        | [Object](https://datatracker.ietf.org/doc/html/rfc7517#page-5)                             |
//! | signing_type      | signing | Define the signing to used for the token validation                | `secret`      | `secret` / `base64secret` / `rsa_pem` / `rsa_components` / `ec_pem` / `rsa_der` / `ec_der` |
//! | key               | -     | Key used for the signing_type                                        | `null`        | String                                                                                     |
//! | payload           | -     | The jwt payload                                                      | `null`        | Object or Array of objects                                                                 |
//! | parameters        | -     | The parameters used to remplace variables in the payload             | `null`        | Object or Array of objects                                                                 |
//! | token             | -     | The token that can be override if necessary                          | `null`        | String                                                                                     |
//!
//! ### Examples
//!
//! ```json
//! [
//!     {
//!         "type": "read",
//!         "connector":{
//!             "type": "mem",
//!             "data": "{\"username\":\"my_username\",\"password\":\"my_password\"}"
//!         }
//!     },
//!     {
//!         "type": "read",
//!         "connector":{
//!             "type": "curl",
//!             "endpoint": "{{ CURL_ENDPOINT }}",
//!             "path": "/my_api",
//!             "method": "get",
//!             "authenticator": {
//!                 "type": "jwt",
//!                 "refresh_connector": {
//!                     "type": "curl",
//!                     "endpoint": "http://my_api.com",
//!                     "path": "/tokens",
//!                     "method": "post"
//!                 },
//!                 "document": {
//!                     "entry_path": "/my_token_field"
//!                 }
//!                 "key": "my_key",
//!                 "payload": {
//!                     "alg":"HS256",
//!                     "claims":{"GivenName":"Johnny","username":"{{ username }}","password":"{{ password }}","iat":1599462755,"exp":33156416077},
//!                     "key":"my_key"
//!                 }
//!             }
//!         }
//!     }
//! ]
//! ```
use super::Authenticator;
use crate::document::Document;
use crate::helper::string::{DisplayOnlyForDebugging, Obfuscate};
use crate::{connector::ConnectorType, document::jsonl::Jsonl};
use async_std::prelude::StreamExt;
use async_std::sync::Arc;
use async_std::sync::Mutex;
use async_trait::async_trait;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::{
    fmt,
    io::{Error, ErrorKind, Result},
};
use surf::http::headers;

static TOKENS: OnceLock<Arc<Mutex<HashMap<String, String>>>> = OnceLock::new();

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Jwt {
    #[serde(alias = "algo")]
    pub algorithm: Algorithm,
    #[serde(rename = "refresh_connector")]
    #[serde(alias = "refresh")]
    pub refresh_connector_type: Option<Box<ConnectorType>>,
    pub document: Box<Jsonl>,
    pub jwk: Option<Value>,
    #[serde(alias = "signing")]
    pub signing_type: Option<SigningType>,
    pub key: String,
    pub payload: Box<Value>,
}

impl fmt::Debug for Jwt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Jwt")
            .field("algorithm", &self.algorithm)
            .field("refresh_connector_type", &self.refresh_connector_type)
            .field("document", &self.document)
            .field("jwk", &self.jwk.display_only_for_debugging())
            .field("signing_type", &self.signing_type)
            .field(
                "key",
                &self
                    .key
                    .to_owned()
                    .to_obfuscate()
                    .display_only_for_debugging(),
            )
            .field("payload", &self.payload.display_only_for_debugging())
            .finish()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum SigningType {
    #[serde(rename = "secret")]
    Secret,
    #[serde(rename = "base64secret")]
    Base64Secret,
    #[serde(rename = "rsa_pem")]
    RsaPem,
    #[serde(rename = "rsa_components")]
    #[serde(alias = "rsa_component")]
    RsaComponents,
    #[serde(rename = "ec_pem")]
    EcPem,
    #[serde(rename = "rsa_der")]
    RsaDer,
    #[serde(rename = "ec_der")]
    EcDer,
}

impl Default for Jwt {
    fn default() -> Self {
        Jwt {
            algorithm: Algorithm::HS256,
            refresh_connector_type: None,
            document: Box::<Jsonl>::default(),
            jwk: None,
            signing_type: None,
            key: "".to_string(),
            payload: Box::new(Value::Null),
        }
    }
}

impl Jwt {
    /// Refresh the jwt
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{Connector, ConnectorType, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt};
    /// use chewdata::Metadata;
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///    let mut connector = Curl::default();
    ///    connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///    connector.path = "/tokens".to_string();
    ///    connector.method = Method::Post;
    ///
    ///    let mut auth = Jwt::default();
    ///    auth.key = "my_key".to_string();
    ///    auth.payload = serde_json::from_str(
    ///        r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
    ///    )?;
    ///    auth.refresh_connector_type = Some(Box::new(ConnectorType::Curl(connector)));
    ///    auth.document.entry_path = Some("/token".to_string());
    ///    auth.document.metadata = Metadata {
    ///        mime_type: Some("application".to_string()),
    ///        mime_subtype: Some("json".to_string()),
    ///        ..Default::default()
    ///    };
    ///    auth.refresh().await?;
    ///
    ///    match auth.refresh().await {
    ///         Ok(_) => (),
    ///         Err(_) => assert!(false, "The token can't be refreshed."),
    ///    };
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "jwt::refresh", skip(self))]
    pub async fn refresh(&self) -> Result<()> {
        let mut connector = match &self.refresh_connector_type {
            Some(refresh_connector_type) => refresh_connector_type.clone().boxed_inner(),
            None => return Ok(()),
        };

        connector.set_metadata(connector.metadata().merge(&self.document.metadata()));
        connector.set_parameters(*self.payload.clone());

        let mut datastream = match connector.fetch(&*self.document).await? {
            Some(datastream) => datastream,
            None => {
                trace!("No data have been retrieve from the refresh endpoint");
                return Ok(());
            }
        };

        let token_value = match datastream.next().await {
            Some(data_result) => data_result.to_value(),
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Can't find JWT in empty data stream",
                ))
            }
        };

        match token_value {
            Value::String(token_value) => {
                let token_key = self.token_key();
                let tokens = TOKENS.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));

                let mut map = tokens.lock_arc().await;
                map.insert(token_key.clone(), token_value.clone());

                info!(
                    token_value = token_value.to_owned().to_obfuscate(),
                    token_key, "JWT refresh with success"
                );

                Ok(())
            }
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "JWT not found in the payload",
            )),
        }?;

        Ok(())
    }
    // Used to verify the signature from the JWT.
    pub fn decode(&self, token_value: &str) -> jsonwebtoken::errors::Result<()> {
        if let Some(signing_type) = &self.signing_type {
            match signing_type {
                SigningType::Secret => decode::<Value>(
                    token_value,
                    &DecodingKey::from_secret(self.key.as_ref()),
                    &Validation::new(self.algorithm),
                ),
                SigningType::Base64Secret => decode::<Value>(
                    token_value,
                    &DecodingKey::from_base64_secret(self.key.as_ref())?,
                    &Validation::new(self.algorithm),
                ),
                SigningType::RsaPem => decode::<Value>(
                    token_value,
                    &DecodingKey::from_rsa_pem(self.key.as_ref())?,
                    &Validation::new(self.algorithm),
                ),
                SigningType::RsaDer => decode::<Value>(
                    token_value,
                    &DecodingKey::from_rsa_der(self.key.as_ref()),
                    &Validation::new(self.algorithm),
                ),
                SigningType::RsaComponents => {
                    let modulus: String = self.jwk.clone().map_or(String::default(), |v| {
                        v.get("n").map_or(String::default(), |a| {
                            a.as_str().map_or(String::default(), |s| s.to_string())
                        })
                    });
                    let exponent: String = self.jwk.clone().map_or(String::default(), |v| {
                        v.get("e").map_or(String::default(), |v| {
                            v.as_str().map_or(String::default(), |s| s.to_string())
                        })
                    });
                    decode::<Value>(
                        token_value,
                        &DecodingKey::from_rsa_components(modulus.as_str(), exponent.as_str())?,
                        &Validation::new(self.algorithm),
                    )
                }
                SigningType::EcDer => decode::<Value>(
                    token_value,
                    &DecodingKey::from_ec_der(self.key.as_ref()),
                    &Validation::new(self.algorithm),
                ),
                SigningType::EcPem => decode::<Value>(
                    token_value,
                    &DecodingKey::from_ec_pem(self.key.as_ref())?,
                    &Validation::new(self.algorithm),
                ),
            }?;
        }

        Ok(())
    }
}

impl Jwt {
    fn token_key(&self) -> String {
        let mut hasher = DefaultHasher::new();
        let key = format!(
            "{:?}:{:?}:{:?}:{:?}",
            self.algorithm,
            self.signing_type,
            self.document.entry_path,
            self.payload.to_string()
        );
        key.hash(&mut hasher);
        hasher.finish().to_string()
    }
    async fn token_stored(&self) -> Result<Option<String>> {
        let token_key = self.token_key();
        let tokens = TOKENS.get_or_init(|| Arc::new(Mutex::new(HashMap::default())));
        Ok(tokens.lock().await.get(&token_key).cloned())
    }
}

#[async_trait]
impl Authenticator for Jwt {
    /// See [`Authenticator::authenticate`] for more details.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::{Connector, ConnectorType, curl::Curl};
    /// use chewdata::document::json::Json;
    /// use chewdata::Metadata;
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt, Authenticator};
    /// use async_std::prelude::*;
    /// use std::io;
    /// use futures::StreamExt;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///     connector.path = "/tokens".to_string();
    ///     connector.method = Method::Post;
    ///
    ///     let mut auth = Jwt::default();
    ///     auth.key = "my_key".to_string();
    ///     auth.payload = serde_json::from_str(
    ///         r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
    ///     ).unwrap();
    ///     auth.refresh_connector_type = Some(Box::new(ConnectorType::Curl(connector)));
    ///     auth.document.entry_path = Some("/token".to_string());
    ///     auth.document.metadata = Metadata {
    ///         mime_type: Some("application".to_string()),
    ///         mime_subtype: Some("json".to_string()),
    ///         ..Default::default()
    ///     };
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Jwt(auth)));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     let datastream = connector.fetch(&document).await.unwrap().unwrap();
    ///     let len = datastream.count().await;
    ///     assert!(0 < len, "Should read one some bytes.");
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument(name = "jwt::authenticate", skip(self))]
    async fn authenticate(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut token_option = self.token_stored().await?;

        {
            if let (None, Some(_)) = (&token_option, &self.refresh_connector_type) {
                self.refresh().await?;
                token_option = self.token_stored().await?;
            }
        }

        {
            if let (Some(token), Some(_)) = (&token_option, &self.refresh_connector_type) {
                match self.decode(token) {
                    Ok(_) => (),
                    Err(e) => {
                        match e.kind() {
                            jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                                self.refresh().await?;
                                token_option = self.token_stored().await?;
                            }
                            _ => {
                                warn!(error = e.to_string().as_str(), "Can't decode the JWT");
                                return Err(Error::new(ErrorKind::InvalidInput, e));
                            }
                        };
                    }
                };
            }
        }

        Ok(match token_option {
            Some(token_value) => {
                let bearer = token_value;
                (
                    headers::AUTHORIZATION.to_string().into_bytes(),
                    format!("Bearer {}", bearer).into_bytes(),
                )
            }
            None => {
                warn!("No JWT found for the authentication");
                (
                    headers::AUTHORIZATION.to_string().into_bytes(),
                    "Bearer".to_string().into_bytes(),
                )
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::curl::Curl;
    use crate::connector::Connector;
    use crate::document::json::Json;
    use crate::Metadata;
    use http_types::Method;

    #[async_std::test]
    async fn refresh_with_jwt_builder() {
        let mut connector = Curl::default();
        connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
        connector.path = "/tokens".to_string();
        connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.payload = serde_json::from_str(
            r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
        ).unwrap();
        auth.refresh_connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.document.entry_path = Some("/token".to_string());
        auth.document.metadata = Metadata {
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };

        match auth.refresh().await {
            Ok(_) => (),
            Err(_) => assert!(false, "The token can't be refreshed."),
        };
    }
    #[async_std::test]
    async fn refresh_with_keycloak() {
        let mut connector = Curl::default();
        connector.endpoint =
            "http://localhost:8083/auth/realms/test/protocol/openid-connect".to_string();
        connector.path = "/token".to_string();
        connector.method = Method::Post;
        connector.timeout = Some(60);

        let mut auth = Jwt::default();
        auth.payload = Box::new(Value::String("client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password".to_string()));
        auth.refresh_connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.document.entry_path = Some("/access_token".to_string());
        auth.document.metadata = Metadata {
            mime_subtype: Some("x-www-form-urlencoded".to_string()),
            ..Default::default()
        };

        match auth.refresh().await {
            Ok(_) => (),
            Err(_) => assert!(false, "The token can't be refreshed."),
        };
    }
    #[async_std::test]
    async fn authenticate_jwt_builder() {
        let mut connector = Curl::default();
        connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
        connector.path = "/tokens".to_string();
        connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.key = "my_key".to_string();
        auth.payload = serde_json::from_str(
            r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
        ).unwrap();
        auth.refresh_connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.document.entry_path = Some("/token".to_string());
        auth.document.metadata = Metadata {
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate().await.unwrap();
        assert_eq!(auth_name, b"authorization");
        assert_eq!(auth_value, b"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJHaXZlbk5hbWUiOiJKb2hubnkiLCJpYXQiOjE1OTk0NjI3NTUsImV4cCI6MzMxNTY0MTYwNzd9.AqlRN2x6T0bE1pJJZ0WPQrmLiK37iT89zlLBiRG5Zu0");
    }
    #[async_std::test]
    async fn authenticate_with_keycloak() {
        let mut jwk_document = Json::default();
        jwk_document.entry_path = Some("/keys".to_string());

        let mut jwk_connector = Curl::default();
        jwk_connector.endpoint =
            "http://localhost:8083/auth/realms/test/protocol/openid-connect".to_string();
        jwk_connector.path = "/certs".to_string();
        jwk_connector.method = Method::Get;
        jwk_connector.timeout = Some(60);

        let mut datastream = jwk_connector.fetch(&jwk_document).await.unwrap().unwrap();
        datastream.next().await.unwrap();
        let jwk = datastream.next().await.unwrap().to_value();

        let mut connector = Curl::default();
        connector.endpoint =
            "http://localhost:8083/auth/realms/test/protocol/openid-connect".to_string();
        connector.path = "/token".to_string();
        connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.algorithm = Algorithm::RS256;
        auth.signing_type = Some(SigningType::RsaComponents);
        auth.jwk = Some(jwk);
        auth.payload = Box::new(Value::String("client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password".to_string()));
        auth.refresh_connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.document.entry_path = Some("/access_token".to_string());
        auth.document.metadata = Metadata {
            mime_subtype: Some("x-www-form-urlencoded".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate().await.unwrap();
        assert_eq!(auth_name, b"authorization");
        assert!(100 < auth_value.len(), "The token is not in a good format");
    }
}
