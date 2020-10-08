use super::Authenticator;
use crate::connector::ConnectorType;
use crate::helper::mustache::Mustache;
use crate::Metadata;
use curl::easy::{Easy, List};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Jwt {
    #[serde(alias = "algo")]
    pub algorithm: Algorithm,
    pub refresh_connector: Option<Box<ConnectorType>>,
    pub refresh_token: String,
    pub jwk: Option<Value>,
    pub format: Format,
    pub key: String,
    pub payload: Value,
    pub parameters: Value,
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
            refresh_token: "token".to_string(),
            jwk: None,
            format: Format::Secret,
            key: "".to_string(),
            payload: Value::Null,
            parameters: Value::Null,
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
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{AuthenticatorType, jwt::Jwt};
    /// use chewdata::connector::{Connector, ConnectorType};
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    /// connector.path = "/tokens".to_string();
    /// connector.method = Method::Post;
    /// connector.can_flush_and_read = true;
    /// let mut jwt = Jwt::default();
    /// jwt.key = "my_key".to_string();
    /// jwt.payload = serde_json::from_str(r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#).expect("Error to parse the json str.");
    /// jwt.refresh_connector = Some(Box::new(ConnectorType::Curl(connector)));
    /// jwt.refresh_token = "token".to_string();
    /// jwt.refresh().expect("Error during the refresh");
    /// assert!(10 < jwt.token.unwrap().len(),"The token should be refresh");
    /// ```
    pub fn refresh(&mut self) -> Result<()> {
        debug!(slog_scope::logger(), "Refresh the jwt token started.");
        if let Some(connector_type) = self.refresh_connector.clone() {
            let metadata = Metadata {
                mime_type: Some(mime::APPLICATION_JSON.to_string()),
                ..Default::default()
            };

            let mut payload = self.payload.clone().to_string();
            let parameters = self.parameters.clone();

            if payload.has_mustache() {
                payload = payload.replace_mustache(parameters);
            }

            let mut connector = connector_type.connector_inner();
            connector.set_metadata(metadata);
            connector.set_flush_and_read(true);
            connector.write_all(payload.as_bytes())?;
            connector.flush()?;

            let mut buf = String::default();
            connector.read_to_string(&mut buf)?;

            if buf.is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Can't find a jwt token on empty response body.",
                ));
            }

            let payload: Value = serde_json::from_str(buf.as_str())?;

            match payload.get(self.refresh_token.clone()) {
                Some(Value::String(token)) => {
                    info!(slog_scope::logger(), "JWT refreshed with succes"; "token" => token);
                    self.token = Some(token.clone());
                    Ok(())
                }
                _ => Err(Error::new(
                    ErrorKind::InvalidInput,
                    "The jwt token not found in the payload.",
                )),
            }?;
        };

        debug!(slog_scope::logger(), "Refresh the jwt token ended.");
        Ok(())
    }
    pub fn decode(&self, token: &str) -> Result<jsonwebtoken::TokenData<Value>> {
        match self.format.clone() {
            Format::Secret => decode::<Value>(
                &token,
                &DecodingKey::from_secret(self.key.as_ref()),
                &Validation::new(self.algorithm),
            )
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
            Format::Base64Secret => decode::<Value>(
                &token,
                &DecodingKey::from_base64_secret(self.key.as_ref())
                    .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?,
                &Validation::new(self.algorithm),
            )
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
            Format::RsaPem => decode::<Value>(
                &token,
                &DecodingKey::from_rsa_pem(self.key.as_ref())
                    .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?,
                &Validation::new(self.algorithm),
            )
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
            Format::RsaDer => decode::<Value>(
                &token,
                &DecodingKey::from_rsa_der(self.key.as_ref()),
                &Validation::new(self.algorithm),
            )
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
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
                    &token,
                    &DecodingKey::from_rsa_components(modulus.as_str(), exponent.as_str()),
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))
            }
            Format::EcDer => decode::<Value>(
                &token,
                &DecodingKey::from_ec_der(self.key.as_ref()),
                &Validation::new(self.algorithm),
            )
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
            Format::EcPem => decode::<Value>(
                &token,
                &DecodingKey::from_ec_pem(self.key.as_ref())
                    .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?,
                &Validation::new(self.algorithm),
            )
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
        }
    }
}

impl Authenticator for Jwt {
    /// Add authentification to a request and connect the system to a document protected by jwt.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::authenticator::{Authenticator, jwt::Jwt};
    /// use curl::easy::{Easy, List};
    ///
    /// let token = "token".to_string();
    /// let mut auth = Jwt::new(token);
    ///
    /// let mut client = Easy::new();
    /// let mut headers = List::new();
    ///
    /// auth.add_authentication(&mut client, &mut headers);
    /// assert_eq!("Authorization: Bearer token", String::from_utf8(headers.iter().next().unwrap().to_vec()).unwrap());
    /// ```
    /// # Example: Set token with parameters
    /// ```
    /// use chewdata::connector::authenticator::{Authenticator, jwt::Jwt};
    /// use curl::easy::{Easy, List};
    /// use serde_json::Value;
    ///
    /// let token = "{{ token }}".to_string();
    /// let mut auth = Jwt::new(token);
    ///
    /// let mut parameters: Value = serde_json::from_str(r#"{"token":"my_token"}"#).unwrap();
    /// auth.set_parameters(parameters);
    ///
    /// let mut client = Easy::new();
    /// let mut headers = List::new();
    ///
    /// auth.add_authentication(&mut client, &mut headers);
    /// assert_eq!("Authorization: Bearer my_token", String::from_utf8(headers.iter().next().unwrap().to_vec()).unwrap());
    /// ```
    fn add_authentication(&mut self, _client: &mut Easy, headers: &mut List) -> Result<()> {
        let mut token_option = self.token.clone();
        let parameters = self.parameters.clone();

        if let (None, Some(_)) = (token_option.clone(), self.refresh_connector.clone()) {
            self.refresh()?;
            headers.append(
                format!(
                    "Authorization: Bearer {}",
                    self.token.clone().unwrap_or_else(|| "".to_string())
                )
                .as_ref(),
            )?;
            return Ok(());
        }

        if let Some(token) = token_option.clone() {
            if token.has_mustache() {
                token_option = Some(token.replace_mustache(parameters.clone()));
            }
        }

        if let (Some(token), Some(_)) = (token_option.clone(), self.refresh_connector.clone()) {
            match self.decode(token.as_ref()) {
                Ok(jwt_payload) => {
                    if self.payload.to_string().has_mustache()
                        && !self
                            .payload
                            .clone()
                            .to_string()
                            .replace_mustache(parameters)
                            .eq(&jwt_payload.claims)
                    {
                        self.refresh()?;
                        token_option = self.token.clone();
                    }
                }
                Err(e) => {
                    warn!(slog_scope::logger(), "Can't decode the jwt";"error"=>e.to_string());
                    self.refresh()?;
                    token_option = self.token.clone();
                }
            };
        }

        if let Some(token) = token_option {
            headers.append(format!("Authorization: Bearer {}", token).as_ref())?;
        }

        Ok(())
    }
    fn set_parameters(&mut self, parameters: Value) {
        self.parameters = parameters;
    }
}
