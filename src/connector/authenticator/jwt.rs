use super::Authenticator;
use crate::document::Document;
use crate::helper::mustache::Mustache;
use crate::DataResult;
use crate::{connector::ConnectorType, document::jsonl::Jsonl};
use async_std::prelude::StreamExt;
use async_trait::async_trait;
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
    pub refresh_connector: Option<Box<ConnectorType>>,
    pub refresh_document: Box<Jsonl>,
    #[serde(alias = "refresh_token")]
    pub refresh_token_name: String,
    pub jwk: Option<Value>,
    pub format: Format,
    pub key: String,
    pub payload: Box<Value>,
    pub token: Option<String>,
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

        let mut obfuscate_token = self.token.clone().unwrap_or_default();
        obfuscate_token.replace_range(
            0..(obfuscate_token.len() / 2),
            (0..(obfuscate_token.len() / 2))
                .map(|_| "#")
                .collect::<String>()
                .as_str(),
        );

        f.debug_struct("Jwt")
            .field("algorithm", &self.algorithm)
            .field("refresh_connector", &self.refresh_connector)
            .field("refresh_document", &self.refresh_document)
            .field("refresh_token_name", &self.refresh_token_name)
            .field("jwk", &self.jwk)
            .field("format", &self.format)
            .field("key", &obfuscate_key)
            .field("payload", &self.payload)
            .field("token", &obfuscate_token)
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
            refresh_connector: None,
            refresh_document: Box::new(Jsonl::default()),
            refresh_token_name: "token".to_string(),
            jwk: None,
            format: Format::Secret,
            key: "".to_string(),
            payload: Box::new(Value::Null),
            token: None,
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
    /// let token = "jwt".to_string();
    ///
    /// let mut auth = Jwt::new(token.clone());
    ///
    /// assert_eq!(token, auth.token.unwrap());
    /// ```
    pub fn new(token: String) -> Self {
        Jwt {
            token: Some(token),
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
    ///    let mut refresh_connector = Curl::default();
    ///    refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///    refresh_connector.path = "/tokens".to_string();
    ///    refresh_connector.method = Method::Post;
    ///
    ///    let mut auth = Jwt::default();
    ///    auth.key = "my_key".to_string();
    ///    auth.payload = serde_json::from_str(
    ///        r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
    ///    ).unwrap();
    ///    auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
    ///    auth.refresh_token_name = "token".to_string();
    ///    auth.refresh_document.metadata = Metadata {
    ///        mime_type: Some("application".to_string()),
    ///        mime_subtype: Some("json".to_string()),
    ///        ..Default::default()
    ///    };
    ///    auth.refresh(Value::Null).await.unwrap();
    ///
    ///    assert!(
    ///        10 < auth.token.unwrap().len(),
    ///        "The token should be refresh"
    ///    );
    ///
    ///     Ok(())
    /// }
    /// ```
    #[instrument]
    pub async fn refresh(&mut self, parameters: Value) -> Result<()> {
        let refresh_connector_type = match self.refresh_connector.clone() {
            Some(refresh_connector_type) => refresh_connector_type,
            None => return Ok(()),
        };

        let mut payload = *self.payload.clone();

        if payload.has_mustache() {
            payload.replace_mustache(parameters);
        }

        let mut refresh_connector = refresh_connector_type.boxed_inner();
        refresh_connector.set_metadata(
            refresh_connector
                .metadata()
                .merge(self.refresh_document.metadata()),
        );

        let dataset = vec![DataResult::Ok(payload)];

        let mut datastream = match refresh_connector
            .send(&*self.refresh_document, &dataset)
            .await?
        {
            Some(datastream) => datastream,
            None => {
                trace!("No data have been fetch from the refresh endpoint");
                return Ok(());
            }
        };

        let payload = match datastream.next().await {
            Some(data_result) => data_result.to_value(),
            None => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Can't find a jwt token in empty data stream",
                ))
            }
        };

        match payload.get(self.refresh_token_name.clone()) {
            Some(Value::String(token)) => {
                info!(token = token.as_str(), "JWT refreshed with success");
                self.token = Some(token.clone());
                Ok(())
            }
            _ => Err(Error::new(
                ErrorKind::InvalidInput,
                "The jwt token not found in the payload",
            )),
        }?;

        Ok(())
    }
    pub fn decode(
        &self,
        token: &str,
    ) -> jsonwebtoken::errors::Result<jsonwebtoken::TokenData<Value>> {
        match self.format.clone() {
            Format::Secret => decode::<Value>(
                token,
                &DecodingKey::from_secret(self.key.as_ref()),
                &Validation::new(self.algorithm),
            ),
            Format::Base64Secret => decode::<Value>(
                token,
                &DecodingKey::from_base64_secret(self.key.as_ref())?,
                &Validation::new(self.algorithm),
            ),
            Format::RsaPem => decode::<Value>(
                token,
                &DecodingKey::from_rsa_pem(self.key.as_ref())?,
                &Validation::new(self.algorithm),
            ),
            Format::RsaDer => decode::<Value>(
                token,
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
                    token,
                    &DecodingKey::from_rsa_components(modulus.as_str(), exponent.as_str())?,
                    &Validation::new(self.algorithm),
                )
            }
            Format::EcDer => decode::<Value>(
                token,
                &DecodingKey::from_ec_der(self.key.as_ref()),
                &Validation::new(self.algorithm),
            ),
            Format::EcPem => decode::<Value>(
                token,
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
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let document = Json::default();
    ///
    ///     let mut refresh_connector = Curl::default();
    ///     refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///     refresh_connector.path = "/tokens".to_string();
    ///     refresh_connector.method = Method::Post;
    ///
    ///     let mut auth = Jwt::default();
    ///     auth.key = "my_key".to_string();
    ///     auth.payload = serde_json::from_str(
    ///         r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
    ///     ).unwrap();
    ///     auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
    ///     auth.refresh_token_name = "token".to_string();
    ///     auth.refresh_document.metadata = Metadata {
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
    async fn authenticate(&mut self, parameters: Value) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut token_option = self.token.clone();

        if let (None, Some(_)) = (token_option.clone(), self.refresh_connector.clone()) {
            self.refresh(parameters.clone()).await?;
            token_option = self.token.clone();
        }

        if let Some(token) = token_option.clone() {
            if token.has_mustache() {
                let mut token = token;
                token.replace_mustache(parameters.clone());
                token_option = Some(token);
            }
        }

        if let (Some(token), Some(_)) = (token_option.clone(), self.refresh_connector.clone()) {
            match self.decode(token.as_ref()) {
                Ok(jwt_payload) => {
                    let mut claim_payload =
                        self.payload.get("claims").unwrap_or(&Value::Null).clone();

                    if claim_payload.has_mustache() {
                        claim_payload.replace_mustache(parameters.clone());
                    }

                    if !claim_payload.eq(&jwt_payload.claims) {
                        token_option = self.token.clone();
                    }
                }
                Err(e) => {
                    match e.kind() {
                        jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                            self.refresh(parameters).await?;
                            token_option = self.token.clone();
                        }
                        _ => {
                            self.token = None;
                            warn!(
                                error = e.to_string().as_str(),
                                "Can't decode the Java Web Token"
                            );
                            return Err(Error::new(ErrorKind::InvalidInput, e));
                        }
                    };
                }
            };
        }

        Ok(match token_option {
            Some(token) => {
                let bearer = base64::encode(token);
                (
                    headers::AUTHORIZATION.to_string().into_bytes(),
                    format!("Bearer {}", bearer).into_bytes(),
                )
            }
            None => {
                warn!("No Java Web Token found for the authentication");
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
        let token = "jwt".to_string();
        let auth = Jwt::new(token.clone());
        assert_eq!(token, auth.token.unwrap());
    }
    #[async_std::test]
    async fn refresh_with_jwt_builder() {
        let mut refresh_connector = Curl::default();
        refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
        refresh_connector.path = "/tokens".to_string();
        refresh_connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.key = "my_key".to_string();
        auth.payload = serde_json::from_str(
            r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
        ).unwrap();
        auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
        auth.refresh_token_name = "token".to_string();
        auth.refresh_document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };
        auth.refresh(Value::Null).await.unwrap();

        assert!(
            10 < auth.token.unwrap().len(),
            "The token should be refresh"
        );
    }
    #[async_std::test]
    async fn refresh_with_keycloak() {
        let mut refresh_connector = Curl::default();
        refresh_connector.endpoint =
            "http://localhost:8083/auth/realms/test/protocol/openid-connect".to_string();
        refresh_connector.path = "/token".to_string();
        refresh_connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.payload = Box::new(Value::String("client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password".to_string()));
        auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
        auth.refresh_token_name = "access_token".to_string();
        auth.refresh_document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("x-www-form-urlencoded".to_string()),
            ..Default::default()
        };
        auth.refresh(Value::Null).await.unwrap();

        assert!(
            10 < auth.token.unwrap().len(),
            "The token should be refresh"
        );
    }
    #[async_std::test]
    async fn authenticate_jwt_builder() {
        let mut refresh_connector = Curl::default();
        refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
        refresh_connector.path = "/tokens".to_string();
        refresh_connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.key = "my_key".to_string();
        auth.payload = serde_json::from_str(
            r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
        ).unwrap();
        auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
        auth.refresh_token_name = "token".to_string();
        auth.refresh_document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate(Value::Null).await.unwrap();
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

        let mut refresh_connector = Curl::default();
        refresh_connector.endpoint =
            "http://localhost:8083/auth/realms/test/protocol/openid-connect".to_string();
        refresh_connector.path = "/token".to_string();
        refresh_connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.algorithm = Algorithm::RS256;
        auth.format = Format::RsaComponents;
        auth.jwk = Some(jwk);
        auth.payload = Box::new(Value::String("client_id=client-test&client_secret=my_secret&scope=openid&username=obiwan&password=yoda&grant_type=password".to_string()));
        auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
        auth.refresh_token_name = "access_token".to_string();
        auth.refresh_document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("x-www-form-urlencoded".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate(Value::Null).await.unwrap();
        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert!(100 < auth_value.len(), "The token is not in a good format");
    }
    #[async_std::test]
    async fn authenticate_with_token_in_param() {
        let parameters: Value =
            serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#).unwrap();
        let mut refresh_connector = Curl::default();
        refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
        refresh_connector.path = "/tokens".to_string();
        refresh_connector.method = Method::Post;

        let mut auth = Jwt::default();
        auth.key = "my_key".to_string();
        auth.payload = serde_json::from_str(
            r#"{"alg":"HS256","claims":{"GivenName":"Johnny","username":"{{ username }}","password":"{{ password }}","iat":1599462755,"exp":33156416077},"key":"my_key"}"#,
        ).unwrap();
        auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
        auth.refresh_token_name = "token".to_string();
        auth.refresh_document.metadata = Metadata {
            mime_type: Some("application".to_string()),
            mime_subtype: Some("json".to_string()),
            ..Default::default()
        };

        let (auth_name, auth_value) = auth.authenticate(parameters).await.unwrap();
        assert_eq!(auth_name, "authorization".to_string().into_bytes());
        assert_eq!(auth_value, "Bearer ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SkhhWFpsYms1aGJXVWlPaUpLYjJodWJua2lMQ0oxYzJWeWJtRnRaU0k2SW0xNVgzVnpaWEp1WVcxbElpd2ljR0Z6YzNkdmNtUWlPaUp0ZVY5d1lYTnpkMjl5WkNJc0ltbGhkQ0k2TVRVNU9UUTJNamMxTlN3aVpYaHdJam96TXpFMU5qUXhOakEzTjMwLmc4bUdyZk5LLThudVQ3dENOSERxbHJVa3c3V3l3Z1ZUQy04V3VIUHBaNmc=".as_bytes().to_vec());
    }
}
