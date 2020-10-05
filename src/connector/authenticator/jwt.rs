use super::Authenticate;
use crate::connector::Connector;
use curl::easy::{Easy, List};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub struct Jwt {
    #[serde(alias = "algo")]
    pub algorithm: Algorithm,
    // Alias where store the token
    pub alias: String,
    pub refresh: Option<Box<Connector>>,
    pub refresh_token_field: String,
    pub jwk: Option<Value>,
    pub format: Format,
    pub key: String,
    pub claims: Value,
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
            alias: "JWT".to_string(),
            refresh: None,
            refresh_token_field: "token".to_string(),
            jwk: None,
            format: Format::Secret,
            key: "".to_string(),
            claims: Value::Null,
        }
    }
}

impl Jwt {
    /// Add authentification to a request and connect the system to a document protected by basic auth.
    ///
    /// # Example: Should authenticate the http call
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{Authenticator, basic::Basic};
    /// use chewdata::connector::Connect;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let username = "my_username";
    /// let password = "my_password";
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator = Some(Authenticator::Basic(Basic::new(username, password)));
    /// connector.method = Method::Get;
    /// connector.path = format!("/basic-auth/{}/{}", username, password);
    /// let mut buffer = String::default();
    /// let len = connector.read_to_string(&mut buffer).unwrap();
    /// assert!(0 < len, "Should read one some bytes.");
    /// ```
    /// # Example: failed the authentification
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{Authenticator, basic::Basic};
    /// use chewdata::connector::Connect;
    /// use std::io::Read;
    /// use serde_json::Value;
    ///
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://localhost:8080".to_string();
    /// connector.authenticator = Some(Authenticator::Basic(Basic::new("bad_username", "bad_password")));
    /// connector.method = Method::Get;
    /// connector.path = "/basic-auth/true_username/true_password".to_string();
    /// let mut buffer = String::default();
    /// match connector.read_to_string(&mut buffer) {
    ///     Ok(_) => assert!(false, "Should generate an error."),
    ///     Err(_) => assert!(true),
    /// };
    /// ```
    pub fn new() -> Self {
        Jwt {
            ..Default::default()
        }
    }
    /// Refresh the jwt
    ///
    /// # Example
    /// ```
    /// use chewdata::connector::curl::{Curl, Method};
    /// use chewdata::connector::authenticator::{Authenticator, jwt::Jwt};
    /// use chewdata::connector::{Connect, Connector};
    /// use std::io::Read;
    /// use serde_json::Value;
    /// use std::env;
    ///
    /// let mut connector = Curl::default();
    /// connector.endpoint = "http://jwtbuilder.jamiekurtz.com".to_string();
    /// connector.path = "/tokens".to_string();
    /// connector.method = Method::Post;
    /// connector.flush_and_read = true;
    /// let mut jwt = Jwt::default();
    /// jwt.alias = "MY_JWT".to_string();
    /// jwt.key = "my_key".to_string();
    /// jwt.claims = serde_json::from_str(r#"{"alg":"HS256","claims":{"GivenName":"Johnny","iat":1599462755,"exp":33156416077},"key":"my_key"}"#).expect("Error to parse the json str.");
    /// jwt.refresh = Some(Box::new(Connector::Curl(connector)));
    /// jwt.refresh_token_field = "token".to_string();
    /// jwt.refresh().expect("Error during the refresh");
    /// let token = env::var("MY_JWT").expect("The token is not found.");
    /// assert!(10 < token.len(),"The token should be refresh");
    /// ```
    pub fn refresh(&self) -> Result<()> {
        trace!(slog_scope::logger(), "Refresh the jwt token.");
        match self.refresh.clone() {
            Some(connector_type) => {
                let mut connector = connector_type.inner();
                connector.set_mime_type(mime::APPLICATION_JSON);
                connector.write_all(self.claims.clone().to_string().as_bytes())?;
                connector.flush()?;

                let mut buf = String::default();
                connector.read_to_string(&mut buf)?;

                if 0 == buf.len() {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Can't find a jwt token on empty response body.",
                    ));
                }

                let payload: Value = serde_json::from_str(buf.as_str())?;

                match payload.get(self.refresh_token_field.clone()) {
                    Some(Value::String(token)) => Ok(env::set_var(self.alias.clone(), token)),
                    _ => Err(Error::new(
                        ErrorKind::InvalidInput,
                        "The jwt token not found in the payload.",
                    )),
                }?
            }
            None => (),
        };

        trace!(slog_scope::logger(), "Refresh the jwt token ended.");
        Ok(())
    }
    pub fn decode(&self, token: &str) -> Result<()> {
        match self.format.clone() {
            Format::Secret => {
                decode::<Value>(
                    &token,
                    &DecodingKey::from_secret(self.key.as_ref()),
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            }
            Format::Base64Secret => {
                decode::<Value>(
                    &token,
                    &DecodingKey::from_base64_secret(self.key.as_ref())
                        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?,
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            }
            Format::RsaPem => {
                decode::<Value>(
                    &token,
                    &DecodingKey::from_rsa_pem(self.key.as_ref())
                        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?,
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            }
            Format::RsaDer => {
                decode::<Value>(
                    &token,
                    &DecodingKey::from_rsa_der(self.key.as_ref()),
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            }
            Format::RsaComponents => {
                let modulus: String = self.jwk.clone().map_or(String::default(), |v| {
                    v.clone().get("n").map_or(String::default(), |a| {
                        a.as_str().map_or(String::default(), |s| s.to_string())
                    })
                });
                let exponent: String = self.jwk.clone().map_or(String::default(), |v| {
                    v.clone().get("e").map_or(String::default(), |v| {
                        v.as_str().map_or(String::default(), |s| s.to_string())
                    })
                });
                decode::<Value>(
                    &token,
                    &DecodingKey::from_rsa_components(modulus.as_str(), exponent.as_str()),
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            }
            Format::EcDer => {
                decode::<Value>(
                    &token,
                    &DecodingKey::from_ec_der(self.key.as_ref()),
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            }
            Format::EcPem => {
                decode::<Value>(
                    &token,
                    &DecodingKey::from_ec_pem(self.key.as_ref())
                        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?,
                    &Validation::new(self.algorithm),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
            }
        };
        Ok(())
    }
}

impl Authenticate for Jwt {
    fn add_authentication(&self, _client: &mut Easy, headers: &mut List) -> Result<()> {
        let mut token_option = match env::var(self.alias.clone()) {
            Ok(token) => Some(token),
            Err(_) => None,
        };

        match (token_option.clone(), self.refresh.clone()) {
            (None, Some(_)) => {
                self.refresh()?;

                token_option = match env::var(self.alias.clone()) {
                    Ok(token) => Some(token),
                    Err(_) => None,
                };
            }
            (Some(token), Some(_)) => {
                match self.decode(token.as_ref()) {
                    Ok(_) => (),
                    Err(e) => {
                        warn!(slog_scope::logger(), "Can't decode the jwt";"error"=>e.to_string());
                        self.refresh()?;

                        token_option = match env::var(self.alias.clone()) {
                            Ok(token) => Some(token),
                            Err(_) => None,
                        };
                    }
                };
            }
            _ => (),
        };

        if let Some(token) = token_option.clone() {
            headers.append(format!("Authorization: Bearer {}", token).as_ref())?;
        }

        Ok(())
    }
}
