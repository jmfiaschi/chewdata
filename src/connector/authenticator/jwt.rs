use super::Authenticator;
use crate::{connector::ConnectorType, document::json::Json};
use crate::document::Document;
use crate::helper::mustache::Mustache;
use async_trait::async_trait;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};
use surf::{http::headers, RequestBuilder};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Jwt {
    #[serde(alias = "algo")]
    pub algorithm: Algorithm,
    pub refresh_connector: Option<Box<ConnectorType>>,
    refresh_document: Box<Json>,
    pub refresh_token: String,
    pub jwk: Option<Value>,
    pub format: Format,
    pub key: String,
    pub payload: Box<Value>,
    pub parameters: Box<Value>,
    pub token: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum Format {
    Secret,
    Base64Secret,
    RsaPem,
    RsaComponents,
    EcPem,
    RsaDer,
    EcDer,
}

impl Default for Jwt {
    fn default() -> Self {
        Jwt {
            algorithm: Algorithm::HS256,
            refresh_connector: None,
            refresh_document: Box::new(Json::default()),
            refresh_token: "token".to_string(),
            jwk: None,
            format: Format::Secret,
            key: "".to_string(),
            payload: Box::new(Value::Null),
            parameters: Box::new(Value::Null),
            token: None,
        }
    }
}

impl Jwt {
    /// Get new jwt.
    ///
    /// # Example
    /// ```
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
    /// # Example
    /// ```
    /// use chewdata::connector::{Connector, ConnectorType, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt};
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut refresh_connector = Curl::default();
    ///     refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///     refresh_connector.path = "/tokens".to_string();
    ///     refresh_connector.method = Method::Post;
    ///
    ///     let mut auth = Jwt::default();
    ///     auth.key = "my_key".to_string();
    ///     auth.payload = serde_json::from_str(r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#)?;
    ///     auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
    ///     auth.refresh_token = "token".to_string();
    ///     auth.refresh().await?;
    ///     assert!(10 < auth.token.unwrap().len(),"The token should be refresh");
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn refresh(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Refresh the jwt token started");
        if let Some(refresh_connector_type) = self.refresh_connector.clone() {
            let mut payload = self.payload.clone();
            let parameters = self.parameters.clone();

            if payload.to_string().has_mustache() {
                payload = serde_json::from_str(
                    payload.to_string().replace_mustache(*parameters).as_str(),
                )?;
            }

            let mut refresh_connector = refresh_connector_type.connector();
            self.refresh_document.write_data(&mut *refresh_connector, *payload).await?;
            refresh_connector.set_metadata(refresh_connector.metadata().merge(self.refresh_document.metadata()));
            refresh_connector.send(None).await?;

            if refresh_connector.inner().is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Can't find a jwt token on empty response body",
                ));
            }

            let payload: Value = serde_json::from_slice(refresh_connector.inner().as_slice())?;

            match payload.get(self.refresh_token.clone()) {
                Some(Value::String(token)) => {
                    info!(slog_scope::logger(), "JWT refreshed with succes"; "token" => token);
                    self.token = Some(token.clone());
                    Ok(())
                }
                _ => Err(Error::new(
                    ErrorKind::InvalidInput,
                    "The jwt token not found in the payload",
                )),
            }?;
        };

        debug!(slog_scope::logger(), "Refresh the jwt token ended");
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
                    &DecodingKey::from_rsa_components(modulus.as_str(), exponent.as_str()),
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
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::{Connector, ConnectorType, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut refresh_connector = Curl::default();
    ///     refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///     refresh_connector.path = "/tokens".to_string();
    ///     refresh_connector.method = Method::Post;
    ///
    ///     let mut auth = Jwt::default();
    ///     auth.key = "my_key".to_string();
    ///     auth.payload = serde_json::from_str(r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#)?;
    ///     auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
    ///     auth.refresh_token = "token".to_string();
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Jwt(auth)));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     connector.fetch().await?;
    ///     let mut buffer = String::default();
    ///     let len = connector.read_to_string(&mut buffer).await?;
    ///     assert!(0 < len, "Should read one some bytes.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: failed the authentification
    /// ```
    /// use chewdata::connector::{Connector, ConnectorType, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut refresh_connector = Curl::default();
    ///     refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///     refresh_connector.path = "/tokens".to_string();
    ///     refresh_connector.method = Method::Post;
    ///
    ///     let mut auth = Jwt::default();
    ///     auth.key = "my_bad_key".to_string();
    ///     auth.payload = serde_json::from_str(r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#)?;
    ///     auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
    ///     auth.refresh_token = "token".to_string();
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Jwt(auth)));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     match connector.fetch().await {
    ///         Ok(_) => assert!(false, "Should generate an error."),
    ///         Err(_) => assert!(true),
    ///     };
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Set token with parameters
    /// ```
    /// use chewdata::connector::{Connector, ConnectorType, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt};
    /// use serde_json::Value;
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut parameters: Value = serde_json::from_str(r#"{"username":"my_username","password":"my_password"}"#)?;
    ///
    ///     let mut refresh_connector = Curl::default();
    ///     refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///     refresh_connector.path = "/tokens".to_string();
    ///     refresh_connector.method = Method::Post;
    ///
    ///     let mut auth = Jwt::default();
    ///     auth.key = "my_key".to_string();
    ///     auth.payload = serde_json::from_str(r#"{"alg":"HS256","claims":{"GivenName":"Johnny","username":"{{ username }}","password":"{{ password }}","iat":1599462755,"exp":33156416077},"key":"my_key"}"#)?;
    ///     auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
    ///     auth.refresh_token = "token".to_string();
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Jwt(auth)));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     connector.parameters = parameters;
    ///     connector.fetch().await?;
    ///     let mut buffer = String::default();
    ///     let len = connector.read_to_string(&mut buffer).await?;
    ///     assert!(0 < len, "Should read one some bytes.");
    ///
    ///     Ok(())
    /// }
    /// ```
    /// # Example: Refresh the expired token without failing
    /// ```
    /// use chewdata::connector::{Connector, ConnectorType, curl::Curl};
    /// use surf::http::Method;
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt};
    /// use async_std::prelude::*;
    /// use std::io;
    ///
    /// #[async_std::main]
    /// async fn main() -> io::Result<()> {
    ///     let mut refresh_connector = Curl::default();
    ///     refresh_connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    ///     refresh_connector.path = "/tokens".to_string();
    ///     refresh_connector.method = Method::Post;
    ///
    ///     let mut auth = Jwt::default();
    ///     auth.key = "my_key".to_string();
    ///     auth.payload = serde_json::from_str(r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1592254044,"exp":1592254044},"key":"my_key"}"#)?;
    ///     auth.refresh_connector = Some(Box::new(ConnectorType::Curl(refresh_connector)));
    ///     auth.refresh_token = "token".to_string();
    ///
    ///     let mut connector = Curl::default();
    ///     connector.endpoint = "http://localhost:8080".to_string();
    ///     connector.authenticator_type = Some(Box::new(AuthenticatorType::Jwt(auth)));
    ///     connector.method = Method::Get;
    ///     connector.path = "/bearer".to_string();
    ///     connector.fetch().await?;
    ///     let mut buffer = String::default();
    ///     let len = connector.read_to_string(&mut buffer).await?;
    ///     assert!(0 < len, "Should read one some bytes.");
    ///
    ///     Ok(())
    /// }
    async fn authenticate(&mut self, request_builder: RequestBuilder) -> Result<RequestBuilder> {
        let mut token_option = self.token.clone();
        let parameters = self.parameters.clone();

        if let (None, Some(_)) = (token_option.clone(), self.refresh_connector.clone()) {
            self.refresh().await?;
            token_option = self.token.clone();
        }

        if let Some(token) = token_option.clone() {
            if token.has_mustache() {
                token_option = Some(token.replace_mustache(*parameters.clone()));
            }
        }

        if let (Some(token), Some(_)) = (token_option.clone(), self.refresh_connector.clone()) {
            match self.decode(token.as_ref()) {
                Ok(jwt_payload) => {
                    if self.payload.to_string().has_mustache()
                        && !self
                            .payload
                            .get("claims")
                            .unwrap_or(&Value::Null)
                            .clone()
                            .to_string()
                            .replace_mustache(*parameters.clone())
                            .eq(&jwt_payload.claims)
                    {
                        token_option = self.token.clone();
                    }
                }
                Err(e) => {
                    match e.kind() {
                        jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                            self.refresh().await?;
                            token_option = self.token.clone();
                        }
                        _ => {
                            self.token = None;
                            warn!(slog_scope::logger(), "Can't decode the Java Web Token"; "error" => e.to_string());
                            return Err(Error::new(ErrorKind::InvalidInput, e));
                        }
                    };
                }
            };
        }

        Ok(match token_option {
            Some(token) => {
                let bearer = base64::encode(token);
                request_builder.header(headers::AUTHORIZATION, format!("Bearer {}", bearer))
            }
            None => {
                warn!(
                    slog_scope::logger(),
                    "No Java Web Token found for the authentication"
                );
                request_builder
            }
        })
    }
    /// See [`Authenticator::set_parameters`] for more details.
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = Box::new(parameters);
    }
}
