use std::{collections::HashMap, env};

use anyhow::Context;
use http::HeaderValue;
use jsonwebtoken::{decode, errors::Error, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use strum::Display;

pub fn get_user_id(auth_header: Option<&HeaderValue>) -> Result<Claims, anyhow::Error> {
    let auth_header = match auth_header {
        Some(auth_header) => auth_header,
        None => return Err(anyhow::anyhow!("No auth header")),
    };

    let jwt = match auth_header.to_str() {
        // splits "Bearer <jwt>" into ["Bearer", "<jwt>"]
        Ok(jwt) => jwt
            .split(' ')
            .collect::<Vec<&str>>()
            .get(1)
            .context("Invalid auth header")?
            .to_string(),
        Err(_) => return Err(anyhow::anyhow!("Invalid auth header")),
    };

    match auth_jwt(&jwt) {
        Ok(claims) => Ok(claims),
        Err(err) => Err(anyhow::anyhow!(err)),
    }
}

pub fn get_service_key(auth_header: Option<&HeaderValue>) -> Result<ServiceKey, anyhow::Error> {
    let auth_header = match auth_header {
        Some(auth_header) => auth_header,
        None => return Err(anyhow::anyhow!("No auth header")),
    };

    let service_key = match auth_header.to_str() {
        // splits "Bearer <service_key>" into ["Bearer", "<service_key>"]
        Ok(service_key) => service_key
            .split(' ')
            .collect::<Vec<&str>>()
            .get(1)
            .context("Invalid auth header")?
            .to_string(),
        Err(_) => return Err(anyhow::anyhow!("Invalid auth header")),
    };

    let key = env::var("SERVICE_KEY").expect("SERVICE_KEY must be set");

    if service_key == key {
        ServiceKey::new(&service_key).map_err(|_| anyhow::anyhow!("Invalid service key"))
    } else {
        Err(anyhow::anyhow!("Invalid service key"))
    }
}

pub struct ServiceKey(String);

#[derive(Debug, PartialEq, Display)]
pub enum ServiceKeyError {
    InvalidLength,
}

impl ServiceKey {
    // Modified constructor for ServiceKey with validation
    // TODO: add more validation
    pub fn new(s: &str) -> Result<Self, ServiceKeyError> {
        if s.len() == 21 {
            Ok(ServiceKey(s.to_string()))
        } else {
            Err(ServiceKeyError::InvalidLength)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub exp: usize,
    // maps org_id to role
    pub orgs: Option<HashMap<String, String>>,
}

fn auth_jwt(jwt: &str) -> Result<Claims, Error> {
    let key = env::var("JWT_SECRET").unwrap();

    let validation = Validation::new(Algorithm::HS256);
    let token_data =
        match decode::<Claims>(jwt, &DecodingKey::from_secret(key.as_ref()), &validation) {
            Ok(c) => c,
            Err(err) => {
                return Err(err);
            }
        };
    Ok(token_data.claims)
}

// tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_jwt() {
        let jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjIwMTYyMzkwMjIsIm9yZ3MiOnsiYmxhIjoiYWRtaW4ifX0.dVFl6aOXUTEfvBey6HeTnDeaS1w-5UHJRPz8Kl4laeM";
        let jwt_secret = "super-secret-jwt-token-with-at-least-32-characters-long";
        env::set_var("JWT_SECRET", jwt_secret);
        assert!(auth_jwt(jwt).is_ok());
    }
}
