//! JSON Schema validation for capability arguments and outputs
//!
//! Provides comprehensive validation of JSON data against JSON Schema Draft-07.
//! Schemas are located in the `media_defs` table of the cap definition or in the registry.

use crate::media::registry::FabricRegistry;
use crate::media::spec::resolve_media_urn;
use crate::{Cap, CapArg, CapOutput};
use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use thiserror::Error;

/// Schema validation error
#[derive(Error, Debug)]
pub enum SchemaValidationError {
    #[error("Schema compilation failed: {0}")]
    SchemaCompilation(String),

    #[error("Validation failed for argument '{argument}': {details}")]
    MediaValidation { argument: String, details: String },

    #[error("Validation failed for output: {details}")]
    OutputValidation { details: String },

    #[error("Media URN '{media_urn}' could not be resolved: {error}")]
    MediaUrnNotResolved { media_urn: String, error: String },

    #[error("Invalid JSON value for validation")]
    InvalidJson,
}

/// Schema validator that resolves schemas from media_defs and registry
pub struct SchemaValidator {
    /// Cache of compiled schemas for performance
    schema_cache: HashMap<String, JSONSchema>,
}

/// Trait for resolving external schema references (for legacy/external schemas)
pub trait SchemaResolver: Send + Sync {
    /// Resolve a schema reference to a JSON schema
    fn resolve_schema(&self, schema_ref: &str) -> Result<JsonValue, SchemaValidationError>;
}

impl SchemaValidator {
    /// Create a new schema validator
    pub fn new() -> Self {
        Self {
            schema_cache: HashMap::new(),
        }
    }

    /// Validate all arguments for a capability against their schemas
    pub async fn validate_arguments(
        &mut self,
        cap: &Cap,
        arguments: &[JsonValue],
        registry: &FabricRegistry,
    ) -> Result<(), SchemaValidationError> {
        let args = cap.get_args();

        // Get positional args sorted by position
        let mut positional_args: Vec<(&CapArg, usize)> = args
            .iter()
            .filter_map(|arg| {
                arg.sources.iter().find_map(|s| {
                    if let crate::ArgSource::Position { position } = s {
                        Some((arg, *position))
                    } else {
                        None
                    }
                })
            })
            .collect();
        positional_args.sort_by_key(|(_, pos)| *pos);

        // Validate positional arguments
        for (arg_def, position) in positional_args {
            if let Some(arg_value) = arguments.get(position) {
                self.validate_argument(arg_def, arg_value, registry).await?;
            }
        }

        Ok(())
    }

    /// Validate a single argument against its schema resolved through the registry.
    pub async fn validate_argument(
        &mut self,
        arg_def: &CapArg,
        value: &JsonValue,
        registry: &FabricRegistry,
    ) -> Result<(), SchemaValidationError> {
        let resolved = resolve_media_urn(&arg_def.media_urn, registry)
            .await
            .map_err(|e| SchemaValidationError::MediaUrnNotResolved {
                media_urn: arg_def.media_urn.clone(),
                error: e.to_string(),
            })?;

        // If no schema in the resolved spec, skip validation
        let schema = match resolved.schema {
            Some(s) => s,
            None => return Ok(()),
        };

        self.validate_value_against_schema(&arg_def.media_urn, value, &schema)
    }

    /// Validate output against its schema resolved through the registry.
    pub async fn validate_output(
        &mut self,
        output_def: &CapOutput,
        value: &JsonValue,
        registry: &FabricRegistry,
    ) -> Result<(), SchemaValidationError> {
        let resolved = resolve_media_urn(&output_def.media_urn, registry)
            .await
            .map_err(|e| SchemaValidationError::MediaUrnNotResolved {
                media_urn: output_def.media_urn.clone(),
                error: e.to_string(),
            })?;

        // If no schema in the resolved spec, skip validation
        let schema = match resolved.schema {
            Some(s) => s,
            None => return Ok(()),
        };

        self.validate_value_against_schema("output", value, &schema)
    }

    /// Validate a JSON value against a schema
    fn validate_value_against_schema(
        &mut self,
        name: &str,
        value: &JsonValue,
        schema: &JsonValue,
    ) -> Result<(), SchemaValidationError> {
        let schema_key =
            serde_json::to_string(schema).map_err(|_| SchemaValidationError::InvalidJson)?;

        // Use cached compiled schema or compile new one
        let compiled_schema = if let Some(cached) = self.schema_cache.get(&schema_key) {
            cached
        } else {
            let compiled = JSONSchema::compile(schema)
                .map_err(|e| SchemaValidationError::SchemaCompilation(e.to_string()))?;
            self.schema_cache.insert(schema_key.clone(), compiled);
            self.schema_cache.get(&schema_key).unwrap()
        };

        // Validate the value
        if let Err(validation_errors) = compiled_schema.validate(value) {
            let error_details = validation_errors
                .map(|e| format!("  - {}", e))
                .collect::<Vec<_>>()
                .join("\n");

            if name == "output" {
                return Err(SchemaValidationError::OutputValidation {
                    details: error_details,
                });
            } else {
                return Err(SchemaValidationError::MediaValidation {
                    argument: name.to_string(),
                    details: error_details,
                });
            }
        }

        Ok(())
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple file-based schema resolver for external schemas
pub struct FileSchemaResolver {
    base_path: std::path::PathBuf,
}

impl FileSchemaResolver {
    /// Create a new file-based schema resolver
    pub fn new(base_path: std::path::PathBuf) -> Self {
        Self { base_path }
    }
}

impl SchemaResolver for FileSchemaResolver {
    fn resolve_schema(&self, schema_ref: &str) -> Result<JsonValue, SchemaValidationError> {
        let schema_path = self.base_path.join(schema_ref);
        let schema_content = std::fs::read_to_string(&schema_path).map_err(|_| {
            SchemaValidationError::MediaUrnNotResolved {
                media_urn: schema_ref.to_string(),
                error: "File not found".to_string(),
            }
        })?;

        serde_json::from_str(&schema_content).map_err(|_| {
            SchemaValidationError::MediaUrnNotResolved {
                media_urn: schema_ref.to_string(),
                error: "Invalid JSON".to_string(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::standard::media::MEDIA_STRING;
    use crate::{ArgSource, CapArg, CapUrn};
    use serde_json::json;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:record";{}"#, tags)
    }

    // Helper to create a test registry
    async fn test_registry() -> FabricRegistry {
        FabricRegistry::new_for_test()
    }

    // TEST163: Test argument schema validation succeeds with valid JSON matching schema
    #[tokio::test]
    async fn test163_argument_schema_validation_success() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            },
            "required": ["name"]
        });

        // Seed the registry with the schema-bearing media def; caps no
        // longer carry inline media defs.
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: "media:fmt=json;record;user-data".to_string(),
            media_type: "application/json".to_string(),
            title: "User Data".to_string(),
            profile_uri: Some("https://example.com/schema/user-data".to_string()),
            schema: Some(schema),
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });

        let arg = CapArg::new(
            "media:fmt=json;record;user-data", "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let valid_value = json!({"name": "John", "age": 30});
        assert!(validator
            .validate_argument(&arg, &valid_value, &registry)
            .await
            .is_ok());
    }

    // TEST164: Test argument schema validation fails with JSON missing required fields
    #[tokio::test]
    async fn test164_argument_schema_validation_failure() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        });

        // Seed the registry with the schema-bearing media def; caps no
        // longer carry inline media defs.
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: "media:fmt=json;record;user-data".to_string(),
            media_type: "application/json".to_string(),
            title: "User Data".to_string(),
            profile_uri: Some("https://example.com/schema/user-data".to_string()),
            schema: Some(schema),
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });

        let arg = CapArg::new(
            "media:fmt=json;record;user-data", "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let invalid_value = json!({"age": 30}); // Missing required "name"
        assert!(validator
            .validate_argument(&arg, &invalid_value, &registry)
            .await
            .is_err());
    }

    // TEST165: Test output schema validation succeeds with valid JSON matching schema
    #[tokio::test]
    async fn test165_output_schema_validation_success() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let schema = json!({
            "type": "object",
            "properties": {
                "result": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"}
            },
            "required": ["result"]
        });

        // Seed the registry with the schema-bearing media def; caps no
        // longer carry inline media defs.
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: "media:fmt=json;query-result;record".to_string(),
            media_type: "application/json".to_string(),
            title: "Query Result".to_string(),
            profile_uri: Some("https://example.com/schema/query-result".to_string()),
            schema: Some(schema),
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });

        let output = CapOutput::new("media:fmt=json;query-result;record", "output", "Query result");

        let valid_value = json!({"result": "success", "timestamp": "2023-01-01T00:00:00Z"});
        assert!(validator
            .validate_output(&output, &valid_value, &registry)
            .await
            .is_ok());
    }

    // TEST166: Test validation skipped when resolved media def has no schema
    #[tokio::test]
    async fn test166_skip_validation_without_schema() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        // Seed MEDIA_STRING into the test registry with no schema; the
        // validator should resolve the URN and short-circuit on
        // "no schema → nothing to enforce".
        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: MEDIA_STRING.to_string(),
            media_type: "text/plain".to_string(),
            title: "String".to_string(),
            profile_uri: None,
            schema: None,
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });

        // Argument using built-in spec ID (should resolve from registry, no schema)
        let arg = CapArg::new(
            MEDIA_STRING, "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let value = json!("any string value");
        // Should succeed - MEDIA_STRING resolves from registry and has no schema
        assert!(validator
            .validate_argument(&arg, &value, &registry)
            .await
            .is_ok());
    }

    // TEST167: Test validation fails hard when media URN cannot be resolved from any source
    #[tokio::test]
    async fn test167_unresolvable_media_urn_fails_hard() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        // Argument with unknown media URN - not in media_defs and not in registry
        let arg = CapArg::new(
            "media:completely-unknown-urn-that-does-not-exist", "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let value = json!("test");
        let result = validator.validate_argument(&arg, &value, &registry).await;
        assert!(result.is_err());

        if let Err(SchemaValidationError::MediaUrnNotResolved { media_urn, .. }) = result {
            assert_eq!(
                media_urn,
                "media:completely-unknown-urn-that-does-not-exist"
            );
        } else {
            panic!("Expected MediaUrnNotResolved error");
        }
    }

    // TEST6314: Complex nested schema validation
    #[tokio::test]
    async fn test6314_complex_nested_schema_validation() {
        let registry = test_registry().await;
        let mut validator = SchemaValidator::new();

        let schema = json!({
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "profile": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "settings": {
                                    "type": "object",
                                    "properties": {
                                        "theme": {"type": "string"},
                                        "notifications": {"type": "boolean"}
                                    }
                                }
                            },
                            "required": ["name"]
                        },
                        "permissions": {
                            "type": "array",
                            "items": {"type": "string", "enum": ["read", "write", "admin"]}
                        }
                    },
                    "required": ["profile", "permissions"]
                }
            },
            "required": ["user"]
        });

        registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: "media:user-data;enc=utf-8;record".to_string(),
            media_type: "application/json".to_string(),
            title: "User Data".to_string(),
            profile_uri: None,
            schema: Some(schema),
            description: None,
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });

        let arg = CapArg::new(
            "media:user-data;enc=utf-8;record", "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );

        let valid = json!({
            "user": {
                "profile": {"name": "John Doe", "settings": {"theme": "dark", "notifications": true}},
                "permissions": ["read", "write"]
            }
        });
        assert!(validator
            .validate_argument(&arg, &valid, &registry)
            .await
            .is_ok());

        // Invalid: permission outside the enum.
        let invalid = json!({
            "user": {
                "profile": {"name": "John Doe"},
                "permissions": ["read", "invalid_permission"]
            }
        });
        assert!(validator
            .validate_argument(&arg, &invalid, &registry)
            .await
            .is_err());
    }

    // TEST6317: Media urn resolution with registry
    #[tokio::test]
    async fn test6317_media_urn_resolution_with_registry() {
        use crate::media::spec::resolve_media_urn;
        let registry = FabricRegistry::new_for_test();
        for (urn, mt) in [
            ("media:enc=utf-8", "text/plain"),
            ("media:integer;numeric", "text/plain"),
            ("media:fmt=json;record", "application/json"),
        ] {
            registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
                version: 0,
                urn: urn.to_string(),
                media_type: mt.to_string(),
                title: urn.to_string(),
                profile_uri: None,
                schema: None,
                description: None,
                documentation: None,
                validation: None,
                metadata: None,
                extensions: Vec::new(),
            });
        }
        let resolved = resolve_media_urn("media:enc=utf-8", &registry)
            .await
            .unwrap();
        assert_eq!(resolved.media_type, "text/plain");
        let json_resolved = resolve_media_urn("media:fmt=json;record", &registry)
            .await
            .unwrap();
        assert_eq!(json_resolved.media_type, "application/json");
    }
}
