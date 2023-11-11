//! Authenticate the request with Java Web Token (JWT).
//!
//! ### Configuration
//!
//! | key               | alias | Description                                                          | Default Value | Possible Values                                                                            |
//! | ----------------- | ----- | -------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------ |
//! | type              | -     | Required in order to use this authentication                         | `jwt`         | `jwt`                                                                                      |
//! | algorithm         | algo  | The algorithm used to build the signing                              | `HS256`       | String                                                                                     |
//! | refresh_connector | -     | The connector used to refresh the token                              | `null`        | See [Connectors](#connectors)                                                              |
//! | refresh_token     | -     | The token name used to identify the token into the refresh connector | `token`       | String                                                                                     |
//! | jwk               | -     | The Json web key used to sign                                        | `null`        | [Object](https://datatracker.ietf.org/doc/html/rfc7517#page-5)                             |
//! | format            | -     | Define the type of the key used for the signing                      | `secret`      | `secret` / `base64secret` / `rsa_pem` / `rsa_components` / `ec_pem` / `rsa_der` / `ec_der` |
//! | key               | -     | Key used for the signing                                             | `null`        | String                                                                                     |
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
//!                 "connector": {
//!                     "type": "curl",
//!                     "endpoint": "http://my_api.com",
//!                     "path": "/tokens",
//!                     "method": "post"
//!                 },
//!                 "token_name":"token",
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
use crate::helper::mustache::Mustache;
use crate::{connector::ConnectorType, document::jsonl::Jsonl};
use async_std::prelude::StreamExt;
use async_trait::async_trait;
use base64::Engine;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::{Error, ErrorKind, Result};
use surf::http::headers;

#[derive(Deserialize, Serialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Jwt {
    #[serde(alias = "algo")]
    pub algorithm: Algorithm,
    pub connector_type: Option<Box<ConnectorType>>,
    pub document: Box<Jsonl>,
    pub jwk: Option<Value>,
    pub format: Format,
    pub key: String,
    pub payload: Box<Value>,
    #[serde(alias = "tn")]
    pub token_name: String,
    #[serde(alias = "token")]
    #[serde(alias = "tv")]
    pub token_value: Option<String>,
}

impl fmt::Debug for Jwt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut obfuscate_key = self.key.clone();
        obfuscate_key.replace_range(
            0..(obfuscate_key.len() / 2),
            (0..(obfuscate_key.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );

        let mut obfuscate_token = self.token_value.clone().unwrap_or_default();
        obfuscate_token.replace_range(
            0..(obfuscate_token.len() / 2),
            (0..(obfuscate_token.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );

        f.debug_struct("Jwt")
            .field("algorithm", &self.algorithm)
            .field("connector_type", &self.connector_type)
            .field("document", &self.document)
            .field("token_name", &self.token_name)
            .field("jwk", &self.jwk)
            .field("format", &self.format)
            .field("key", &obfuscate_key)
            .field("payload", &self.payload)
            .field("token_value", &obfuscate_token)
            .finish()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum Format {
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
            connector_type: None,
            document: Box::<Jsonl>::default(),
            jwk: None,
            format: Format::Secret,
            key: "".to_string(),
            payload: Box::new(Value::Null),
            token_name: "token".to_string(),
            token_value: None,
        }
    }
}

impl Jwt {
    /// Get new jwt.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use chewdata::connector::authenticator::jwt::Jwt;
    ///
    /// let token_value = "jwt".to_string();
    ///
    /// let mut auth = Jwt::new(token_value.clone());
    ///
    /// assert_eq!(token_value, auth.token_value.unwrap());
    /// ```
    pub fn new(token_value: String) -> Self {
        Jwt {
            token_value: Some(token_value),
            ..Default::default()
        }
    }
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
    ///    ).unwrap();
    ///    auth.connector_type = Some(Box::new(ConnectorType::Curl(connector)));
    ///    auth.token_name = "token".to_string();
    ///    auth.document.metadata = Metadata {
    ///        mime_type: Some("application".to_string()),
    ///        mime_subtype: Some("json".to_string()),
    ///        ..Default::default()
    ///    };
    ///    auth.refresh(&Value::Null).await.unwrap();
    ///
    ///    assert!(
    ///        10 < auth.token_value.unwrap().len(),
    ///        "The token should be refresh."
    ///    );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    pub async fn refresh(&mut self, parameters: &Value) -> Result<()> {
        let mut connector = match &self.connector_type {
            Some(connector_type) => connector_type.clone().boxed_inner(),
            None => return Ok(()),
        };

        let mut payload = *self.payload.clone();

        if payload.has_mustache() {
            payload.replace_mustache(parameters.clone());
        }

        connector.set_metadata(connector.metadata().merge(&self.document.metadata()));
        connector.set_parameters(payload);

        let mut datastream = match connector.fetch(&*self.document).await? {
            Some(datastream) => datastream,
            None => {
                trace!("No data have been retrieve from the refresh endpoint.");
                return Ok(());
            }
        };

        let payload = match datastream.next().await {
            Some(data_result) => data_result.to_value(),
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Can't find JWT in empty data stream.",
                ))
            }
        };

        match payload.get(&self.token_name) {
            Some(Value::String(token_value)) => {
                info!(
                    token_value = token_value.as_str(),
                    "JWT successfully refreshed."
                );
                self.token_value = Some(token_value.clone());
                Ok(())
            }
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "JWT not found in the payload.",
            )),
        }?;

        Ok(())
    }
    pub fn decode(
        &self,
        token_value: &str,
    ) -> jsonwebtoken::errors::Result<jsonwebtoken::TokenData<Value>> {
        match self.format {
            Format::Secret => decode::<Value>(
                token_value,
                &DecodingKey::from_secret(self.key.as_ref()),
                &Validation::new(self.algorithm),
            ),
            Format::Base64Secret => decode::<Value>(
                token_value,
                &DecodingKey::from_base64_secret(self.key.as_ref())?,
                &Validation::new(self.algorithm),
            ),
            Format::RsaPem => decode::<Value>(
                token_value,
                &DecodingKey::from_rsa_pem(self.key.as_ref())?,
                &Validation::new(self.algorithm),
            ),
            Format::RsaDer => decode::<Value>(
                token_value,
                &DecodingKey::from_rsa_der(self.key.as_ref()),
                &Validation::new(self.algorithm),
            ),
            Format::RsaComponents => {
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
            Format::EcDer => decode::<Value>(
                token_value,
                &DecodingKey::from_ec_der(self.key.as_ref()),
                &Validation::new(self.algorithm),
            ),
            Format::EcPem => decode::<Value>(
                token_value,
                &DecodingKey::from_ec_pem(self.key.as_ref())?,
                &Validation::new(self.algorithm),
            ),
        }
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
    ///     auth.connector_type = Some(Box::new(ConnectorType::Curl(connector)));
    ///     auth.token_name = "token".to_string();
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
    #[instrument]
    async fn authenticate(&mut self, parameters: &Value) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut token_option = self.token_value.clone();

        if let (None, Some(_)) = (&token_option, &self.connector_type) {
            self.refresh(&parameters).await?;
            token_option = self.token_value.clone();
        }

        if let Some(token_value) = &token_option {
            if token_value.has_mustache() {
                let mut token_value = token_value.clone();
                token_value.replace_mustache(parameters.clone());
                token_option = Some(token_value);
            }
        }

        if let (Some(token_value), Some(_)) = (&token_option, &self.connector_type) {
            match self.decode(token_value) {
                Ok(jwt_payload) => {
                    let mut claim_payload =
                        self.payload.get("claims").unwrap_or(&Value::Null).clone();

                    if claim_payload.has_mustache() {
                        claim_payload.replace_mustache(parameters.clone());
                    }

                    if !claim_payload.eq(&jwt_payload.claims) {
                        token_option = self.token_value.clone();
                    }
                }
                Err(e) => {
                    match e.kind() {
                        jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                            self.refresh(parameters).await?;
                            token_option = self.token_value.clone();
                        }
                        _ => {
                            self.token_value = None;
                            warn!(error = e.to_string().as_str(), "Can't decode the JWT.");
                            return Err(Error::new(ErrorKind::InvalidInput, e));
                        }
                    };
                }
            };
        }

        Ok(match token_option {
            Some(token_value) => {
                let bearer = base64::engine::general_purpose::STANDARD.encode(token_value);
                (
                    headers::AUTHORIZATION.to_string().into_bytes(),
                    format!("Bearer {}", bearer).into_bytes(),
                )
            }
            None => {
                warn!("No JWT found for the authentication.");
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

    #[test]
    fn new() {
        let token_value = "jwt".to_string();
        let auth = Jwt::new(token_value.clone());
        assert_eq!(token_value, auth.token_value.unwrap());
    }
    #[async_std::test]
    async fn refresh_with_jwt_builder() {
        let mut connector = Curl::default();
        connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
        connector.path = "/tokens".to_string();
        connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.key = "my_key".to_string();
        auth.payload = serde_json::from_str(
            r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
        ).unwrap();
        auth.connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.token_name = "token".to_string();
        auth.document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };
        auth.refresh(&Value::Null).await.unwrap();

        assert!(
            10 < auth.token_value.unwrap().len(),
            "The token should be refresh"
        );
    }
    #[async_std::test]
    async fn refresh_with_keycloak() {
        let mut connector = Curl::default();
        connector.endpoint =
            "http://localhost:8083/auth/realms/test/protocol/openid-connect".to_string();
        connector.path = "/token".to_string();
        connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.payload = Box::new(Value::String("client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password".to_string()));
        auth.connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.token_name = "access_token".to_string();
        auth.document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("x-www-form-urlencoded".to_string()),
            ..Default::default()
        };
        auth.refresh(&Value::Null).await.unwrap();

        assert!(
            10 < auth.token_value.unwrap().len(),
            "The token should be refresh"
        );
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
        auth.connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.token_name = "token".to_string();
        auth.document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate(&Value::Null).await.unwrap();
        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(auth_value, "Bearer ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SkhhWFpsYms1aGJXVWlPaUpLYjJodWJua2lMQ0pwWVhRaU9qRTFPVGswTmpJM05UVXNJbVY0Y0NJNk16TXhOVFkwTVRZd056ZDkuQXFsUk4yeDZUMGJFMXBKSlowV1BRcm1MaUszN2lUODl6bExCaVJHNVp1MA==".as_bytes().to_vec());
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
        auth.format = Format::RsaComponents;
        auth.jwk = Some(jwk);
        auth.payload = Box::new(Value::String("client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password".to_string()));
        auth.connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.token_name = "access_token".to_string();
        auth.document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("x-www-form-urlencoded".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate(&Value::Null).await.unwrap();
        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert!(100 < auth_value.len(), "The token is not in a good format");
    }
    #[async_std::test]
    async fn authenticate_with_token_in_param() {
        let parameters: Value =
            serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#).unwrap();
        let mut connector = Curl::default();
        connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
        connector.path = "/tokens".to_string();
        connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.key = "my_key".to_string();
        auth.payload = serde_json::from_str(
            r#"{"alg":"HS256","claims":{"GivenName":"Johnny","username":"{{ username }}","password":"{{ password }}","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
        ).unwrap();
        auth.connector_type = Some(Box::new(ConnectorType::Curl(connector)));
        auth.token_name = "token".to_string();
        auth.document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate(&parameters).await.unwrap();
        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(auth_value, "Bearer ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SkhhWFpsYms1aGJXVWlPaUpLYjJodWJua2lMQ0oxYzJWeWJtRnRaU0k2SW0xNVgzVnpaWEp1WVcxbElpd2ljR0Z6YzNkdmNtUWlPaUp0ZVY5d1lYTnpkMjl5WkNJc0ltbGhkQ0k2TVRVNU9UUTJNamMxTlN3aVpYaHdJam96TXpFMU5qUXhOakEzTjMwLmc4bUdyZk5LLThudVQ3dENOSERxbHJVa3c3V3l3Z1ZUQy04V3VIUHBaNmc=".as_bytes().to_vec());
    }
}
