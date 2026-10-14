//! Response wrapper for unified cartridge output handling with validation

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

/// Unified response wrapper for all cartridge operations
/// Provides type-safe deserialization of cartridge output
#[derive(Debug, Clone)]
pub struct ResponseWrapper {
    raw_bytes: Vec<u8>,
    content_type: ResponseContentType,
}

#[derive(Debug, Clone, PartialEq)]
enum ResponseContentType {
    Json,
    Text,
    Binary,
}

impl ResponseWrapper {
    /// Create from JSON output
    pub fn from_json(data: Vec<u8>) -> Self {
        Self {
            raw_bytes: data,
            content_type: ResponseContentType::Json,
        }
    }

    /// Create from text output
    pub fn from_text(data: Vec<u8>) -> Self {
        Self {
            raw_bytes: data,
            content_type: ResponseContentType::Text,
        }
    }

    /// Create from binary output (like PNG images)
    pub fn from_binary(data: Vec<u8>) -> Self {
        Self {
            raw_bytes: data,
            content_type: ResponseContentType::Binary,
        }
    }

    /// Get raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw_bytes
    }

    /// Convert to string
    pub fn as_string(&self) -> Result<String> {
        String::from_utf8(self.raw_bytes.clone())
            .map_err(|e| anyhow!("Failed to convert response to string: {}", e))
    }

    /// Convert to integer
    pub fn as_int(&self) -> Result<i64> {
        let text = self.as_string()?;
        let trimmed = text.trim();

        // Try parsing as JSON number first
        if let Ok(json_val) = serde_json::from_str::<JsonValue>(trimmed) {
            if let Some(num) = json_val.as_i64() {
                return Ok(num);
            }
        }

        // Fall back to direct parsing
        trimmed
            .parse::<i64>()
            .map_err(|e| anyhow!("Failed to parse '{}' as integer: {}", trimmed, e))
    }

    /// Convert to float
    pub fn as_float(&self) -> Result<f64> {
        let text = self.as_string()?;
        let trimmed = text.trim();

        // Try parsing as JSON number first
        if let Ok(json_val) = serde_json::from_str::<JsonValue>(trimmed) {
            if let Some(num) = json_val.as_f64() {
                return Ok(num);
            }
        }

        // Fall back to direct parsing
        trimmed
            .parse::<f64>()
            .map_err(|e| anyhow!("Failed to parse '{}' as float: {}", trimmed, e))
    }

    /// Convert to boolean
    pub fn as_bool(&self) -> Result<bool> {
        let text = self.as_string()?;
        let trimmed = text.trim().to_lowercase();

        match trimmed.as_str() {
            "true" | "1" | "yes" | "y" => Ok(true),
            "false" | "0" | "no" | "n" => Ok(false),
            _ => {
                // Try parsing as JSON boolean
                if let Ok(json_val) = serde_json::from_str::<JsonValue>(&trimmed) {
                    if let Some(bool_val) = json_val.as_bool() {
                        return Ok(bool_val);
                    }
                }
                Err(anyhow!("Failed to parse '{}' as boolean", trimmed))
            }
        }
    }

    /// Deserialize to any type implementing serde::Deserialize
    pub fn as_type<T: DeserializeOwned>(&self) -> Result<T> {
        match self.content_type {
            ResponseContentType::Json => {
                let text = self.as_string()?;
                serde_json::from_str(&text).map_err(|e| {
                    anyhow!(
                        "Failed to deserialize JSON response: {}\\nResponse: {}",
                        e,
                        text
                    )
                })
            }
            ResponseContentType::Text => {
                // For text responses, try to deserialize the string directly
                let text = self.as_string()?;
                serde_json::from_str(&format!("\"{}\"", text.replace("\"", "\\\""))).map_err(|e| {
                    anyhow!(
                        "Failed to deserialize text response as JSON string: {}\\nResponse: {}",
                        e,
                        text
                    )
                })
            }
            ResponseContentType::Binary => Err(anyhow!(
                "Cannot deserialize binary response to structured type"
            )),
        }
    }

    /// Check if response is empty
    pub fn is_empty(&self) -> bool {
        self.raw_bytes.is_empty()
    }

    /// Get response size in bytes
    pub fn size(&self) -> usize {
        self.raw_bytes.len()
    }

    /// Validate response against cap output definition (basic validation)
    /// Get content type for validation purposes
    pub fn get_content_type(&self) -> &str {
        match self.content_type {
            ResponseContentType::Json => "application/json",
            ResponseContentType::Text => "text/plain",
            ResponseContentType::Binary => "application/octet-stream",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestStruct {
        name: String,
        value: i32,
    }

    // TEST168: Test ResponseWrapper from JSON deserializes to correct structured type
    #[test]
    fn test168_json_response() {
        let test_data = TestStruct {
            name: "test".to_string(),
            value: 42,
        };
        let json_str = serde_json::to_string(&test_data).unwrap();
        let response = ResponseWrapper::from_json(json_str.into_bytes());

        let parsed: TestStruct = response.as_type().unwrap();
        assert_eq!(parsed, test_data);
    }

    // TEST169: Test ResponseWrapper converts to primitive types integer, float, boolean, string
    #[test]
    fn test169_primitive_types() {
        // Test integer
        let response = ResponseWrapper::from_text(b"42".to_vec());
        assert_eq!(response.as_int().unwrap(), 42);

        // Test float
        let response = ResponseWrapper::from_text(b"3.14".to_vec());
        assert_eq!(response.as_float().unwrap(), 3.14);

        // Test boolean
        let response = ResponseWrapper::from_text(b"true".to_vec());
        assert_eq!(response.as_bool().unwrap(), true);

        // Test string
        let response = ResponseWrapper::from_text(b"hello world".to_vec());
        assert_eq!(response.as_string().unwrap(), "hello world");
    }

    // TEST170: Test ResponseWrapper from binary stores and retrieves raw bytes correctly
    #[test]
    fn test170_binary_response() {
        let binary_data = vec![0x89, 0x50, 0x4E, 0x47]; // PNG header
        let response = ResponseWrapper::from_binary(binary_data.clone());

        assert_eq!(response.as_bytes(), &binary_data);
        assert_eq!(response.size(), 4);
    }

    // TEST599: is_empty returns true for empty response, false for non-empty
    #[test]
    fn test599_is_empty() {
        let empty_json = ResponseWrapper::from_json(vec![]);
        assert!(empty_json.is_empty());

        let empty_text = ResponseWrapper::from_text(vec![]);
        assert!(empty_text.is_empty());

        let empty_binary = ResponseWrapper::from_binary(vec![]);
        assert!(empty_binary.is_empty());

        let non_empty = ResponseWrapper::from_text(b"x".to_vec());
        assert!(!non_empty.is_empty());
    }

    // TEST600: size returns exact byte count for all content types
    #[test]
    fn test600_size() {
        let text = ResponseWrapper::from_text(b"hello".to_vec());
        assert_eq!(text.size(), 5);

        let json = ResponseWrapper::from_json(b"{}".to_vec());
        assert_eq!(json.size(), 2);

        let binary = ResponseWrapper::from_binary(vec![0u8; 1024]);
        assert_eq!(binary.size(), 1024);

        let empty = ResponseWrapper::from_text(vec![]);
        assert_eq!(empty.size(), 0);
    }

    // TEST601: get_content_type returns correct MIME type for each variant
    #[test]
    fn test601_get_content_type() {
        let json = ResponseWrapper::from_json(b"{}".to_vec());
        assert_eq!(json.get_content_type(), "application/json");

        let text = ResponseWrapper::from_text(b"hello".to_vec());
        assert_eq!(text.get_content_type(), "text/plain");

        let binary = ResponseWrapper::from_binary(vec![0xFF]);
        assert_eq!(binary.get_content_type(), "application/octet-stream");
    }

    // TEST602: as_type on binary response returns error (cannot deserialize binary)
    #[test]
    fn test602_as_type_binary_error() {
        let binary = ResponseWrapper::from_binary(vec![0x89, 0x50]);
        let result: Result<TestStruct, _> = binary.as_type();
        assert!(
            result.is_err(),
            "Binary responses must not be deserializable to structured types"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("binary"),
            "Error should mention binary: {}",
            err
        );
    }

    // TEST603: as_bool handles all accepted truthy/falsy variants and rejects garbage
    #[test]
    fn test603_as_bool_edge_cases() {
        // Truthy values
        for input in &["true", "TRUE", "True", "1", "yes", "YES", "y", "Y"] {
            let resp = ResponseWrapper::from_text(input.as_bytes().to_vec());
            assert!(resp.as_bool().unwrap(), "'{}' should be truthy", input);
        }

        // Falsy values
        for input in &["false", "FALSE", "False", "0", "no", "NO", "n", "N"] {
            let resp = ResponseWrapper::from_text(input.as_bytes().to_vec());
            assert!(!resp.as_bool().unwrap(), "'{}' should be falsy", input);
        }

        // Garbage input should error
        let garbage = ResponseWrapper::from_text(b"maybe".to_vec());
        assert!(garbage.as_bool().is_err());

        // Whitespace-padded should still work
        let padded = ResponseWrapper::from_text(b"  true  ".to_vec());
        assert!(padded.as_bool().unwrap());
    }
}
