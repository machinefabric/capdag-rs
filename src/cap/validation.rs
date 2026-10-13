//! Cap schema validation infrastructure
//!
//! This module provides strict validation of inputs and outputs against
//! cap schemas, ensuring adherence to advertised specifications.
//! Uses spec ID resolution to get media types and schemas from the media_defs table.
//! Uses ProfileSchemaRegistry for JSON Schema-based validation of profiles.

use crate::media::profile::ProfileSchemaRegistry;
use crate::media::spec::{resolve_media_urn, MediaValidation, ResolvedMediaDef};
use crate::{ArgSource, Cap, CapArg, CapOutput};
use serde_json::Value;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

/// Validation error types with descriptive failure information
#[derive(Debug, Clone)]
pub enum ValidationError {
    /// Unknown cap requested
    UnknownCap { cap_urn: String },
    /// Missing required argument
    MissingRequiredArgument {
        cap_urn: String,
        argument_name: String,
    },
    /// Unknown argument provided
    UnknownArgument {
        cap_urn: String,
        argument_name: String,
    },
    /// Invalid argument type
    InvalidArgumentType {
        cap_urn: String,
        argument_name: String,
        expected_media_def: String,
        actual_value: Value,
        schema_errors: Vec<String>,
    },
    /// Media def validation rule violation (inherent to the semantic type)
    MediaDefValidationFailed {
        cap_urn: String,
        argument_name: String,
        media_urn: String,
        validation_rule: String,
        actual_value: Value,
    },
    /// Invalid output type
    InvalidOutputType {
        cap_urn: String,
        expected_media_def: String,
        actual_value: Value,
        schema_errors: Vec<String>,
    },
    /// Output media def validation rule violation (inherent to the semantic type)
    OutputMediaDefValidationFailed {
        cap_urn: String,
        media_urn: String,
        validation_rule: String,
        actual_value: Value,
    },
    /// Malformed cap schema
    InvalidCapSchema { cap_urn: String, issue: String },
    /// Too many arguments provided
    TooManyArguments {
        cap_urn: String,
        max_expected: usize,
        actual_count: usize,
    },
    /// JSON parsing error
    JsonParseError { cap_urn: String, error: String },
    /// JSON schema validation error
    SchemaValidationFailed {
        cap_urn: String,
        field_name: String,
        schema_errors: String,
    },
    /// Invalid MediaDef
    InvalidMediaDef {
        cap_urn: String,
        field_name: String,
        error: String,
    },
    /// Unresolvable Media URN reference
    UnresolvableMediaUrn {
        cap_urn: String,
        media_urn: String,
        location: String,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::UnknownCap { cap_urn } => {
                write!(
                    f,
                    "Unknown cap '{}' - cap not registered or advertised",
                    cap_urn
                )
            }
            ValidationError::MissingRequiredArgument {
                cap_urn,
                argument_name,
            } => {
                write!(
                    f,
                    "Cap '{}' requires argument '{}' but it was not provided",
                    cap_urn, argument_name
                )
            }
            ValidationError::UnknownArgument {
                cap_urn,
                argument_name,
            } => {
                write!(f, "Cap '{}' does not accept argument '{}' - check capability definition for valid arguments", cap_urn, argument_name)
            }
            ValidationError::InvalidArgumentType {
                cap_urn,
                argument_name,
                expected_media_def,
                actual_value,
                schema_errors,
            } => {
                write!(f, "Cap '{}' argument '{}' expects media_def '{}' but validation failed for value {}: {}",
                       cap_urn, argument_name, expected_media_def, actual_value, schema_errors.join(", "))
            }
            ValidationError::MediaDefValidationFailed {
                cap_urn,
                argument_name,
                media_urn,
                validation_rule,
                actual_value,
            } => {
                write!(f, "Cap '{}' argument '{}' failed media def '{}' validation rule '{}' with value: {}",
                       cap_urn, argument_name, media_urn, validation_rule, actual_value)
            }
            ValidationError::InvalidOutputType {
                cap_urn,
                expected_media_def,
                actual_value,
                schema_errors,
            } => {
                write!(
                    f,
                    "Cap '{}' output expects media_def '{}' but validation failed for value {}: {}",
                    cap_urn,
                    expected_media_def,
                    actual_value,
                    schema_errors.join(", ")
                )
            }
            ValidationError::OutputMediaDefValidationFailed {
                cap_urn,
                media_urn,
                validation_rule,
                actual_value,
            } => {
                write!(
                    f,
                    "Cap '{}' output failed media def '{}' validation rule '{}' with value: {}",
                    cap_urn, media_urn, validation_rule, actual_value
                )
            }
            ValidationError::InvalidCapSchema { cap_urn, issue } => {
                write!(f, "Cap '{}' has invalid schema: {}", cap_urn, issue)
            }
            ValidationError::TooManyArguments {
                cap_urn,
                max_expected,
                actual_count,
            } => {
                write!(
                    f,
                    "Cap '{}' expects at most {} arguments but received {}",
                    cap_urn, max_expected, actual_count
                )
            }
            ValidationError::JsonParseError { cap_urn, error } => {
                write!(f, "Cap '{}' JSON parsing failed: {}", cap_urn, error)
            }
            ValidationError::SchemaValidationFailed {
                cap_urn,
                field_name,
                schema_errors,
            } => {
                write!(
                    f,
                    "Cap '{}' schema validation failed for '{}': {}",
                    cap_urn, field_name, schema_errors
                )
            }
            ValidationError::InvalidMediaDef {
                cap_urn,
                field_name,
                error,
            } => {
                write!(
                    f,
                    "Cap '{}' has invalid media_def for '{}': {}",
                    cap_urn, field_name, error
                )
            }
            ValidationError::UnresolvableMediaUrn {
                cap_urn,
                media_urn,
                location,
            } => {
                write!(
                    f,
                    "Cap '{}' references unresolvable media URN '{}' in {}",
                    cap_urn, media_urn, location
                )
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Input argument validator using ProfileSchemaRegistry and FabricRegistry
pub struct InputValidator {
    schema_registry: Arc<ProfileSchemaRegistry>,
    fabric_registry: Arc<crate::FabricRegistry>,
}

impl InputValidator {
    /// Create a new InputValidator with the given registries
    pub fn new(
        schema_registry: Arc<ProfileSchemaRegistry>,
        fabric_registry: Arc<crate::FabricRegistry>,
    ) -> Self {
        Self {
            schema_registry,
            fabric_registry,
        }
    }

    /// Validate arguments against cap input schema
    pub async fn validate_positional_arguments(
        &self,
        cap: &Cap,
        arguments: &[Value],
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let args = cap.get_args();

        // Get positional args sorted by position
        let positional_args: Vec<&CapArg> = args
            .iter()
            .filter(|arg| {
                arg.sources
                    .iter()
                    .any(|s| matches!(s, ArgSource::Position { .. }))
            })
            .collect();

        // Sort by position
        let mut sorted_positional: Vec<(&CapArg, usize)> = positional_args
            .iter()
            .filter_map(|arg| {
                arg.sources.iter().find_map(|s| {
                    if let ArgSource::Position { position } = s {
                        Some((*arg, *position))
                    } else {
                        None
                    }
                })
            })
            .collect();
        sorted_positional.sort_by_key(|(_, pos)| *pos);

        // Check if too many arguments provided
        if arguments.len() > sorted_positional.len() {
            return Err(ValidationError::TooManyArguments {
                cap_urn,
                max_expected: sorted_positional.len(),
                actual_count: arguments.len(),
            });
        }

        // Validate each positional argument
        for (index, (arg_def, _pos)) in sorted_positional.iter().enumerate() {
            if index >= arguments.len() {
                // Missing argument - check if required
                if arg_def.required {
                    return Err(ValidationError::MissingRequiredArgument {
                        cap_urn: cap_urn.clone(),
                        argument_name: arg_def.media_urn.clone(),
                    });
                }
                continue;
            }

            self.validate_single_argument(cap, arg_def, &arguments[index])
                .await?;
        }

        Ok(())
    }

    /// Validate named arguments against cap input schema
    pub async fn validate_named_arguments(
        &self,
        cap: &Cap,
        named_args: &[Value],
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let args = cap.get_args();

        // Extract named argument values into a map (map by media_urn)
        let mut provided_args = std::collections::HashMap::new();
        for arg in named_args {
            if let Value::Object(map) = arg {
                if let (Some(Value::String(media_urn)), Some(value)) =
                    (map.get("name"), map.get("value"))
                {
                    provided_args.insert(media_urn.clone(), value.clone());
                }
            }
        }

        // Check all cap args - match by media_urn
        for arg_def in args {
            if let Some(provided_value) = provided_args.get(&arg_def.media_urn) {
                self.validate_single_argument(cap, arg_def, provided_value)
                    .await?;
            } else if arg_def.required {
                // Check if it has a cli_flag source (meaning it can be provided as named arg)
                let has_cli_flag = arg_def
                    .sources
                    .iter()
                    .any(|s| matches!(s, ArgSource::CliFlag { .. }));
                if has_cli_flag {
                    return Err(ValidationError::MissingRequiredArgument {
                        cap_urn: cap_urn.clone(),
                        argument_name: format!(
                            "{} (expected as named argument)",
                            arg_def.media_urn
                        ),
                    });
                }
            }
        }

        // Check for unknown arguments - match by media_urn
        let known_media_urns: HashSet<String> =
            args.iter().map(|arg| arg.media_urn.clone()).collect();

        for provided_media_urn in provided_args.keys() {
            if !known_media_urns.contains(provided_media_urn) {
                return Err(ValidationError::UnknownArgument {
                    cap_urn: cap_urn.clone(),
                    argument_name: provided_media_urn.clone(),
                });
            }
        }

        Ok(())
    }

    async fn validate_single_argument(
        &self,
        cap: &Cap,
        arg_def: &CapArg,
        value: &Value,
    ) -> Result<(), ValidationError> {
        // Type validation via resolved spec (includes local schema validation if present)
        // Returns the resolved media def so we can access its validation rules
        let resolved = self.validate_argument_type(cap, arg_def, value).await?;

        // Media def validation rules (inherent to the semantic type)
        if let Some(ref validation) = resolved.validation {
            self.validate_media_def_rules(cap, arg_def, &resolved, validation, value)?;
        }

        Ok(())
    }

    async fn validate_argument_type(
        &self,
        cap: &Cap,
        arg_def: &CapArg,
        value: &Value,
    ) -> Result<ResolvedMediaDef, ValidationError> {
        let cap_urn = cap.urn_string();

        // Resolve the spec ID from the argument definition. Caps no
        // longer carry inline media defs; the registry is the only source.
        let _ = cap;
        let resolved = resolve_media_urn(&arg_def.media_urn, &self.fabric_registry)
            .await
            .map_err(|e| ValidationError::InvalidMediaDef {
                cap_urn: cap_urn.clone(),
                field_name: arg_def.media_urn.clone(),
                error: e.to_string(),
            })?;

        // First, try to use local schema from resolved spec
        if let Some(ref schema) = resolved.schema {
            // Validate against the local schema
            if let Err(errors) = self.validate_with_local_schema(schema, value) {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.media_urn.clone(),
                    expected_media_def: arg_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
            return Ok(resolved);
        }

        // Otherwise, validate against profile schema (via ProfileSchemaRegistry)
        if let Some(ref profile) = resolved.profile_uri {
            if let Err(errors) = self.schema_registry.validate(profile, value).await {
                return Err(ValidationError::InvalidArgumentType {
                    cap_urn,
                    argument_name: arg_def.media_urn.clone(),
                    expected_media_def: arg_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
        }
        // No profile or schema means any JSON value is valid for that media type

        Ok(resolved)
    }

    /// Validate value against media def's inherent validation rules (first pass)
    fn validate_media_def_rules(
        &self,
        cap: &Cap,
        arg_def: &CapArg,
        resolved: &ResolvedMediaDef,
        validation: &MediaValidation,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();

        // The decision is the media definition's (`MediaValidation::violation`);
        // this gives it the cap and the argument it was made for.
        match validation.violation(value) {
            Ok(None) => Ok(()),
            Ok(Some(validation_rule)) => Err(ValidationError::MediaDefValidationFailed {
                cap_urn,
                argument_name: arg_def.media_urn.clone(),
                media_urn: resolved.media_urn.clone(),
                validation_rule,
                actual_value: value.clone(),
            }),
            Err(issue) => Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("{} in media def '{}'", issue, resolved.media_urn),
            }),
        }
    }

    /// Validate a value against a local JSON schema
    fn validate_with_local_schema(&self, schema: &Value, value: &Value) -> Result<(), Vec<String>> {
        // Use jsonschema crate for validation
        let compiled = match jsonschema::JSONSchema::compile(schema) {
            Ok(c) => c,
            Err(e) => return Err(vec![format!("Failed to compile schema: {}", e)]),
        };

        let result = compiled.validate(value);
        match result {
            Ok(_) => Ok(()),
            Err(errors) => {
                let error_strings: Vec<String> = errors
                    .map(|e| format!("{}: {}", e.instance_path, e))
                    .collect();
                Err(error_strings)
            }
        }
    }
}

/// Output validator using ProfileSchemaRegistry and FabricRegistry
pub struct OutputValidator {
    schema_registry: Arc<ProfileSchemaRegistry>,
    fabric_registry: Arc<crate::FabricRegistry>,
}

impl OutputValidator {
    /// Create a new OutputValidator with the given registries
    pub fn new(
        schema_registry: Arc<ProfileSchemaRegistry>,
        fabric_registry: Arc<crate::FabricRegistry>,
    ) -> Self {
        Self {
            schema_registry,
            fabric_registry,
        }
    }

    /// Validate output against cap output schema
    pub async fn validate_output(&self, cap: &Cap, output: &Value) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();

        let output_def = cap
            .main_output()
            .ok_or_else(|| ValidationError::InvalidCapSchema {
                cap_urn: cap_urn.clone(),
                issue: "No output definition specified".to_string(),
            })?;

        // Type validation via resolved spec (includes local schema validation if present)
        // Returns the resolved media def so we can access its validation rules
        let resolved = self.validate_output_type(cap, output_def, output).await?;

        // Media def validation rules (inherent to the semantic type)
        if let Some(ref validation) = resolved.validation {
            self.validate_output_media_def_rules(cap, &resolved, validation, output)?;
        }

        Ok(())
    }

    async fn validate_output_type(
        &self,
        cap: &Cap,
        output_def: &CapOutput,
        value: &Value,
    ) -> Result<ResolvedMediaDef, ValidationError> {
        let cap_urn = cap.urn_string();

        // Resolve the spec ID from the output definition. Caps no longer
        // carry inline media defs; the registry is the only source.
        let _ = cap;
        let resolved = resolve_media_urn(&output_def.media_urn, &self.fabric_registry)
            .await
            .map_err(|e| ValidationError::InvalidMediaDef {
                cap_urn: cap_urn.clone(),
                field_name: "output".to_string(),
                error: e.to_string(),
            })?;

        // First, try to use local schema from resolved spec
        if let Some(ref schema) = resolved.schema {
            // Validate against the local schema
            if let Err(errors) = self.validate_with_local_schema(schema, value) {
                return Err(ValidationError::InvalidOutputType {
                    cap_urn,
                    expected_media_def: output_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
            return Ok(resolved);
        }

        // Otherwise, validate against profile schema (via ProfileSchemaRegistry)
        if let Some(ref profile) = resolved.profile_uri {
            if let Err(errors) = self.schema_registry.validate(profile, value).await {
                return Err(ValidationError::InvalidOutputType {
                    cap_urn,
                    expected_media_def: output_def.media_urn.clone(),
                    actual_value: value.clone(),
                    schema_errors: errors,
                });
            }
        }
        // No profile or schema means any JSON value is valid for that media type

        Ok(resolved)
    }

    /// Validate output value against media def's inherent validation rules (first pass)
    fn validate_output_media_def_rules(
        &self,
        cap: &Cap,
        resolved: &ResolvedMediaDef,
        validation: &MediaValidation,
        value: &Value,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();

        // The same decision an argument value is held to.
        match validation.violation(value) {
            Ok(None) => Ok(()),
            Ok(Some(validation_rule)) => Err(ValidationError::OutputMediaDefValidationFailed {
                cap_urn,
                media_urn: resolved.media_urn.clone(),
                validation_rule,
                actual_value: value.clone(),
            }),
            Err(issue) => Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("{} in media def '{}'", issue, resolved.media_urn),
            }),
        }
    }

    /// Validate a value against a local JSON schema
    fn validate_with_local_schema(&self, schema: &Value, value: &Value) -> Result<(), Vec<String>> {
        // Use jsonschema crate for validation
        let compiled = match jsonschema::JSONSchema::compile(schema) {
            Ok(c) => c,
            Err(e) => return Err(vec![format!("Failed to compile schema: {}", e)]),
        };

        let result = compiled.validate(value);
        match result {
            Ok(_) => Ok(()),
            Err(errors) => {
                let error_strings: Vec<String> = errors
                    .map(|e| format!("{}: {}", e.instance_path, e))
                    .collect();
                Err(error_strings)
            }
        }
    }
}

/// Cap schema validator
pub struct CapValidator;

impl CapValidator {
    /// Validate a cap definition itself
    pub async fn validate_cap(
        cap: &Cap,
        registry: &crate::media::registry::FabricRegistry,
    ) -> Result<(), ValidationError> {
        let cap_urn = cap.urn_string();
        let args = cap.get_args();

        // Validate that required arguments don't have default values
        for arg in args {
            if arg.required && arg.default_value.is_some() {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn: cap_urn.clone(),
                    issue: format!(
                        "Required argument '{}' cannot have a default value",
                        arg.media_urn
                    ),
                });
            }
        }

        // Validate argument position uniqueness
        let mut positions = std::collections::HashSet::new();
        for arg in args {
            for source in &arg.sources {
                if let ArgSource::Position { position } = source {
                    if !positions.insert(*position) {
                        return Err(ValidationError::InvalidCapSchema {
                            cap_urn: cap_urn.clone(),
                            issue: format!(
                                "Duplicate argument position {} for argument '{}'",
                                position, arg.media_urn
                            ),
                        });
                    }
                }
            }
        }

        // Validate CLI flag uniqueness
        let mut cli_flags = std::collections::HashSet::new();
        for arg in args {
            for source in &arg.sources {
                if let ArgSource::CliFlag { cli_flag } = source {
                    if !cli_flag.is_empty() {
                        if !cli_flags.insert(cli_flag) {
                            return Err(ValidationError::InvalidCapSchema {
                                cap_urn: cap_urn.clone(),
                                issue: format!(
                                    "Duplicate CLI flag '{}' for argument '{}'",
                                    cli_flag, arg.media_urn
                                ),
                            });
                        }
                    }
                }
            }
        }

        // Validate that all media URN references can be resolved through
        // the registry. Caps no longer carry inline media defs.
        for arg in args {
            resolve_media_urn(&arg.media_urn, registry)
                .await
                .map_err(|e| ValidationError::InvalidMediaDef {
                    cap_urn: cap_urn.clone(),
                    field_name: arg.media_urn.clone(),
                    error: e.to_string(),
                })?;
        }

        if let Some(output) = cap.main_output() {
            resolve_media_urn(&output.media_urn, registry)
                .await
                .map_err(|e| ValidationError::InvalidMediaDef {
                    cap_urn: cap_urn.clone(),
                    field_name: "output".to_string(),
                    error: e.to_string(),
                })?;
        }

        Ok(())
    }
}

/// Reserved CLI flags that cannot be used
pub const RESERVED_CLI_FLAGS: &[&str] = &["manifest", "--help", "--version", "-v", "-h"];

/// Whether `name` may label an argument or an output (RULE17, RULE19): the
/// lexical form of a machine-notation alias, and not one of the words the
/// notation reads as a constant.
pub fn is_valid_definition_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !matches!(name, "true" | "false" | "null")
}

/// Validate a cap definition: its arguments ([`validate_cap_args`]) and its
/// outputs ([`validate_cap_outputs`]). The rules are numbered as in
/// `capdag/docs/10-validation-rules.md`; a number names one rule.
pub fn validate_cap_definition(cap: &Cap) -> Result<(), ValidationError> {
    validate_cap_args(cap)?;
    validate_cap_outputs(cap)
}

/// Validate a cap's outputs.
///
/// # Validation Rules
/// - RULE19: Every output has a well-formed name, unique among the cap's outputs
/// - RULE20: No duplicate output media_urns
/// - RULE21: `out=media:void` declares no outputs; otherwise exactly one
///   output's media_urn is equivalent to `out=` (the main output)
/// - RULE22: `carries = "input"` only on a non-main output of a cap that takes an input
pub fn validate_cap_outputs(cap: &Cap) -> Result<(), ValidationError> {
    let cap_urn = cap.urn_string();
    let schema = |issue: String| ValidationError::InvalidCapSchema {
        cap_urn: cap_urn.clone(),
        issue,
    };
    let void_media = crate::MediaUrn::from_string(crate::urn::media_urn::MEDIA_VOID)
        .expect("MEDIA_VOID is a valid MediaUrn");
    let out_media = cap
        .urn
        .out_media_urn()
        .map_err(|e| schema(format!("RULE21: Failed to parse out= spec: {}", e)))?;
    let in_media = cap
        .urn
        .in_media_urn()
        .map_err(|e| schema(format!("RULE22: Failed to parse in= spec: {}", e)))?;
    let out_is_void = out_media.is_equivalent(&void_media).unwrap_or(false);
    let in_is_void = in_media.is_equivalent(&void_media).unwrap_or(false);

    // RULE19: names
    let mut names: Vec<(&str, &str)> = Vec::new();
    for output in &cap.outputs {
        if output.name.is_empty() {
            return Err(schema(format!("RULE19: Output '{}' has no name", output.media_urn)));
        }
        if !is_valid_definition_name(&output.name) {
            return Err(schema(format!(
                "RULE19: Output name '{}' is not a valid name",
                output.name
            )));
        }
        if let Some((_, other)) = names.iter().find(|(name, _)| *name == output.name) {
            return Err(schema(format!(
                "RULE19: Outputs '{}' and '{}' share the name '{}'",
                other, output.media_urn, output.name
            )));
        }
        names.push((&output.name, &output.media_urn));
    }

    // RULE20: identities, compared as media URNs (tag order is not identity)
    let mut identities: Vec<crate::MediaUrn> = Vec::new();
    for output in &cap.outputs {
        let urn = crate::MediaUrn::from_string(&output.media_urn).map_err(|e| {
            schema(format!(
                "RULE20: Output media_urn '{}' is not a media URN: {}",
                output.media_urn, e
            ))
        })?;
        if identities.iter().any(|seen| seen.is_equivalent(&urn).unwrap_or(false)) {
            return Err(schema(format!(
                "RULE20: Duplicate output media_urn '{}'",
                output.media_urn
            )));
        }
        identities.push(urn);
    }

    // RULE21: the main output
    if out_is_void {
        if !cap.outputs.is_empty() {
            return Err(schema(format!(
                "RULE21: out= is 'media:void' but {} output(s) are declared",
                cap.outputs.len()
            )));
        }
        return Ok(());
    }
    let main: Vec<usize> = identities
        .iter()
        .enumerate()
        .filter(|(_, urn)| urn.is_equivalent(&out_media).unwrap_or(false))
        .map(|(index, _)| index)
        .collect();
    if main.len() != 1 {
        return Err(schema(format!(
            "RULE21: out= is '{}' but {} outputs are equivalent to it; exactly one must be",
            cap.urn.out_spec(),
            main.len()
        )));
    }

    // RULE22: what an output carries
    for (index, output) in cap.outputs.iter().enumerate() {
        if output.carries.is_none() {
            continue;
        }
        if index == main[0] {
            return Err(schema(format!(
                "RULE22: The main output '{}' declares carries",
                output.media_urn
            )));
        }
        if in_is_void {
            return Err(schema(format!(
                "RULE22: Output '{}' carries the input but the cap takes none",
                output.media_urn
            )));
        }
    }
    Ok(())
}

/// Validate cap args against the argument rules
///
/// # Validation Rules
/// - RULE1: No duplicate media_urns
/// - RULE2: sources must not be null or empty
/// - RULE3: At most one arg declares a stdin source
/// - RULE4: No arg may specify same source type more than once
/// - RULE5: No two args may have same position
/// - RULE6: Positions must be sequential (0-based, no gaps when aggregated)
/// - RULE7: No arg may have both position and cli_flag
/// - RULE8: No unknown keys in source objects (enforced by serde)
/// - RULE9: No two args may have same cli_flag
/// - RULE10: Reserved cli_flags cannot be used
/// - RULE11: cli_flag used verbatim as specified (enforced by design)
/// - RULE12: media_urn is the key; a name is a label (enforced by what bindings key on)
/// - RULE14: Only the main input argument may declare streaming=true
/// - RULE15: The stdin argument's URN is `in=`; `in=media:void` has none
/// - RULE17: Every arg has a well-formed name, unique among the cap's args
/// - RULE18: `accepts` never beside a stdin source
pub fn validate_cap_args(cap: &Cap) -> Result<(), ValidationError> {
    let cap_urn = cap.urn_string();
    let args = cap.get_args();

    // RULE1: No duplicate media_urns
    let mut media_urns = HashSet::new();
    for arg in args {
        if !media_urns.insert(&arg.media_urn) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE1: Duplicate media_urn '{}'", arg.media_urn),
            });
        }
    }

    // RULE2: sources must not be null or empty
    for arg in args {
        if arg.sources.is_empty() {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE2: Argument '{}' has empty sources", arg.media_urn),
            });
        }
    }

    // Collect stdin URNs, positions, and cli_flags for cross-arg validation
    let mut stdin_urns: Vec<String> = Vec::new();
    let mut positions: Vec<(usize, String)> = Vec::new();
    let mut cli_flags: Vec<(String, String)> = Vec::new();

    for arg in args {
        let mut source_types = HashSet::new();
        let mut has_position = false;
        let mut has_cli_flag = false;

        for source in &arg.sources {
            let source_type = source.get_type();

            // RULE4: No arg may specify same source type more than once
            if !source_types.insert(source_type) {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn,
                    issue: format!(
                        "RULE4: Argument '{}' has duplicate source type '{}'",
                        arg.media_urn, source_type
                    ),
                });
            }

            match source {
                ArgSource::Stdin { stdin } => {
                    stdin_urns.push(stdin.clone());
                }
                ArgSource::Position { position } => {
                    has_position = true;
                    positions.push((*position, arg.media_urn.clone()));
                }
                ArgSource::CliFlag { cli_flag } => {
                    has_cli_flag = true;
                    cli_flags.push((cli_flag.clone(), arg.media_urn.clone()));

                    // RULE10: Reserved cli_flags
                    if RESERVED_CLI_FLAGS.contains(&cli_flag.as_str()) {
                        return Err(ValidationError::InvalidCapSchema {
                            cap_urn,
                            issue: format!(
                                "RULE10: Argument '{}' uses reserved cli_flag '{}'",
                                arg.media_urn, cli_flag
                            ),
                        });
                    }
                }
            }
        }

        // RULE7: No arg may have both position and cli_flag
        if has_position && has_cli_flag {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!(
                    "RULE7: Argument '{}' has both position and cli_flag sources",
                    arg.media_urn
                ),
            });
        }
    }

    // RULE3: At most one arg declares a stdin source. `stdin` is a command
    // line's one piped input, and the arg that declares it is the main input.
    let stdin_args: Vec<&str> = args
        .iter()
        .filter(|arg| arg.sources.iter().any(|s| matches!(s, ArgSource::Stdin { .. })))
        .map(|arg| arg.media_urn.as_str())
        .collect();
    if stdin_args.len() > 1 {
        return Err(ValidationError::InvalidCapSchema {
            cap_urn,
            issue: format!(
                "RULE3: Arguments '{}' and '{}' both declare a stdin source; a cap has one main input",
                stdin_args[0], stdin_args[1]
            ),
        });
    }

    // RULE17: names
    let mut names: Vec<(&str, &str)> = Vec::new();
    for arg in args {
        if arg.name.is_empty() {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE17: Argument '{}' has no name", arg.media_urn),
            });
        }
        if !is_valid_definition_name(&arg.name) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!("RULE17: Argument name '{}' is not a valid name", arg.name),
            });
        }
        if let Some((_, other)) = names.iter().find(|(name, _)| *name == arg.name) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!(
                    "RULE17: Arguments '{}' and '{}' share the name '{}'",
                    other, arg.media_urn, arg.name
                ),
            });
        }
        names.push((&arg.name, &arg.media_urn));
    }

    // RULE18: an arg says what it accepts in one place.
    for arg in args {
        if arg.accepts.is_some() && arg.sources.iter().any(|s| matches!(s, ArgSource::Stdin { .. })) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!(
                    "RULE18: Argument '{}' declares both a stdin source and accepts",
                    arg.media_urn
                ),
            });
        }
    }

    // RULE15: The main input matches in=.
    // If in= is media:void, no arg may have a stdin source (the cap takes no data-flow input).
    // Otherwise exactly one does (RULE3 caps it at one), and its URN is in=.
    let in_media = cap
        .urn
        .in_media_urn()
        .map_err(|e| ValidationError::InvalidCapSchema {
            cap_urn: cap_urn.clone(),
            issue: format!("RULE15: Failed to parse in= spec: {}", e),
        })?;
    let void_media = crate::MediaUrn::from_string(crate::urn::media_urn::MEDIA_VOID)
        .expect("MEDIA_VOID is a valid MediaUrn");
    let in_is_void = in_media.is_equivalent(&void_media).unwrap_or(false);

    if in_is_void && !stdin_urns.is_empty() {
        return Err(ValidationError::InvalidCapSchema {
            cap_urn,
            issue: format!(
                "RULE15: in= is 'media:void' but {} arg(s) declare a stdin source",
                stdin_urns.len()
            ),
        });
    }
    if !in_is_void {
        // The stdin arg IS the main input, so its URN is `in=` — compared as
        // tagged URNs, never as strings.
        let matches_in = stdin_urns.iter().any(|stdin| {
            crate::MediaUrn::from_string(stdin)
                .map(|urn| urn.is_equivalent(&in_media).unwrap_or(false))
                .unwrap_or(false)
        });
        if !matches_in {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!(
                    "RULE15: in= is '{}' but no arg declares a stdin source equivalent to it",
                    cap.urn.in_spec()
                ),
            });
        }
    }

    // RULE14: Only the main input may stream. `streaming: true` declares that an
    // argument is consumed without a length promise — a feed. Side arguments
    // (options, model specs, prompts alongside the main input) are values the
    // runtime demuxes whole; a feed there has no meaning and would let an
    // unbounded stream reach a collector that must refuse it (L16).
    for arg in &cap.args {
        if arg.streaming && !arg.is_main_input(&in_media) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn: cap_urn.clone(),
                issue: format!(
                    "RULE14: Argument '{}' declares streaming=true but is not the main input \
                     (no stdin source equivalent to in='{}') — only the main input may be \
                     consumed without a length promise",
                    arg.media_urn,
                    cap.urn.in_spec()
                ),
            });
        }
    }

    // RULE5: No two args may have same position
    let mut position_set = HashSet::new();
    for (position, media_urn) in &positions {
        if !position_set.insert(*position) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!(
                    "RULE5: Duplicate position {} in argument '{}'",
                    position, media_urn
                ),
            });
        }
    }

    // RULE6: Positions must be sequential (0-based, no gaps when aggregated)
    if !positions.is_empty() {
        let mut sorted_positions = positions.clone();
        sorted_positions.sort_by_key(|(pos, _)| *pos);
        for (i, (position, _)) in sorted_positions.iter().enumerate() {
            if *position != i {
                return Err(ValidationError::InvalidCapSchema {
                    cap_urn,
                    issue: format!(
                        "RULE6: Position gap - expected {} but found {}",
                        i, position
                    ),
                });
            }
        }
    }

    // RULE9: No two args may have same cli_flag
    let mut flag_set = HashSet::new();
    for (flag, media_urn) in &cli_flags {
        if !flag_set.insert(flag) {
            return Err(ValidationError::InvalidCapSchema {
                cap_urn,
                issue: format!(
                    "RULE9: Duplicate cli_flag '{}' in argument '{}'",
                    flag, media_urn
                ),
            });
        }
    }

    // RULE8: No unknown keys in source objects - enforced by serde(deny_unknown_fields)
    // RULE11: cli_flag used verbatim as specified - enforced by design
    // RULE12: media_urn is the key, no name field - enforced by CapArg structure

    Ok(())
}

/// Main validation coordinator that orchestrates input and output validation
#[derive(Debug, Clone)]
pub struct SchemaValidator {
    caps: std::collections::HashMap<String, Cap>,
}

impl SchemaValidator {
    pub fn new() -> Self {
        Self {
            caps: std::collections::HashMap::new(),
        }
    }

    /// Register a cap schema for validation
    pub fn register_cap(&mut self, cap: Cap) {
        let urn = cap.urn_string();
        self.caps.insert(urn, cap);
    }

    /// Get a cap by URN
    pub fn get_cap(&self, cap_urn: &str) -> Option<&Cap> {
        self.caps.get(cap_urn)
    }

    /// Validate arguments against a cap's input schema
    pub async fn validate_inputs(
        &self,
        cap_urn: &str,
        arguments: &[serde_json::Value],
        schema_registry: Arc<ProfileSchemaRegistry>,
        fabric_registry: Arc<crate::FabricRegistry>,
    ) -> Result<(), ValidationError> {
        let cap = self
            .get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        let validator = InputValidator::new(schema_registry, fabric_registry);
        validator
            .validate_positional_arguments(cap, arguments)
            .await
    }

    /// Validate output against a cap's output schema
    pub async fn validate_output(
        &self,
        cap_urn: &str,
        output: &serde_json::Value,
        schema_registry: Arc<ProfileSchemaRegistry>,
        fabric_registry: Arc<crate::FabricRegistry>,
    ) -> Result<(), ValidationError> {
        let cap = self
            .get_cap(cap_urn)
            .ok_or_else(|| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;

        let validator = OutputValidator::new(schema_registry, fabric_registry);
        validator.validate_output(cap, output).await
    }

    /// Validate a cap definition itself
    pub async fn validate_cap_schema(
        &self,
        cap: &Cap,
        fabric_registry: Arc<crate::FabricRegistry>,
    ) -> Result<(), ValidationError> {
        CapValidator::validate_cap(cap, &fabric_registry).await
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::registry::FabricRegistry;
    use crate::standard::media::{MEDIA_INTEGER, MEDIA_STRING};
    use crate::CapUrn;
    use serde_json::json;

    // Helper to create test URN with required in/out specs
    fn test_urn(tags: &str) -> String {
        format!(r#"cap:in="media:void";out="media:record";{}"#, tags)
    }

    // Helper to create test registries
    async fn test_registries() -> (Arc<ProfileSchemaRegistry>, Arc<FabricRegistry>) {
        let schema_registry = Arc::new(
            ProfileSchemaRegistry::new()
                .await
                .expect("Failed to create schema registry"),
        );
        let fabric_registry = Arc::new(FabricRegistry::new_for_test());
        (schema_registry, fabric_registry)
    }

    // TEST051: Test input validation succeeds with valid positional argument
    #[tokio::test]
    async fn test051_input_validation_success() {
        let (schema_registry, fabric_registry) = test_registries().await;
        // Seed MEDIA_STRING so the validator can resolve the cap's arg
        // URN to a media def under the test registry's empty manifest.
        fabric_registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
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
        let validator = InputValidator::new(schema_registry, fabric_registry);

        let urn = CapUrn::from_string(&test_urn("type=test;cap")).unwrap();
        let mut cap = Cap::new(
            urn,
            "Test Capability".to_string(),
            vec!["test-command".to_string()],
        );

        let arg = CapArg::new(
            MEDIA_STRING, "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );
        cap.add_arg(arg);

        let input_args = vec![json!("/path/to/file.txt")];

        assert!(validator
            .validate_positional_arguments(&cap, &input_args)
            .await
            .is_ok());
    }

    // TEST052: Test input validation fails with MissingRequiredArgument when required arg missing
    #[tokio::test]
    async fn test052_input_validation_missing_required() {
        let (schema_registry, fabric_registry) = test_registries().await;
        let validator = InputValidator::new(schema_registry, fabric_registry);

        let urn = CapUrn::from_string(&test_urn("type=test;cap")).unwrap();
        let mut cap = Cap::new(
            urn,
            "Test Capability".to_string(),
            vec!["test-command".to_string()],
        );

        let arg = CapArg::new(
            MEDIA_STRING, "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );
        cap.add_arg(arg);

        let input_args = vec![]; // Missing required argument

        let result = validator
            .validate_positional_arguments(&cap, &input_args)
            .await;
        assert!(result.is_err());

        if let Err(ValidationError::MissingRequiredArgument { argument_name, .. }) = result {
            assert_eq!(argument_name, MEDIA_STRING);
        } else {
            panic!("Expected MissingRequiredArgument error");
        }
    }

    // TEST053: Test input validation fails with InvalidArgumentType when wrong type provided
    #[tokio::test]
    async fn test053_input_validation_wrong_type() {
        let (schema_registry, fabric_registry) = test_registries().await;
        let validator = InputValidator::new(schema_registry, Arc::clone(&fabric_registry));

        let urn = CapUrn::from_string(&test_urn("type=test;cap")).unwrap();
        let mut cap = Cap::new(
            urn,
            "Test Capability".to_string(),
            vec!["test-command".to_string()],
        );

        // Seed the registry with the schema-bearing media def; caps no
        // longer carry inline media defs.
        fabric_registry.insert_cached_media_def_for_test(crate::StoredMediaDef {
            version: 0,
            urn: MEDIA_INTEGER.to_string(),
            media_type: "text/plain".to_string(),
            title: "Integer".to_string(),
            profile_uri: Some("https://capdag.com/schema/integer".to_string()),
            schema: Some(json!({"type": "integer"})),
            description: Some("Integer value".to_string()),
            documentation: None,
            validation: None,
            metadata: None,
            extensions: Vec::new(),
        });

        let arg = CapArg::new(
            MEDIA_INTEGER, "arg0",
            true,
            vec![ArgSource::Position { position: 0 }],
        );
        cap.add_arg(arg);

        let input_args = vec![json!("not_a_number")]; // Wrong type

        let result = validator
            .validate_positional_arguments(&cap, &input_args)
            .await;
        assert!(result.is_err());

        if let Err(ValidationError::InvalidArgumentType { .. }) = result {
            // Expected
        } else {
            panic!("Expected InvalidArgumentType error");
        }
    }

    // TEST054 / TEST055 / TEST056 (XV5: inline media def redefinition)
    // were deleted along with `cap.media_defs`. Caps no longer carry
    // inline media defs, so the conditions those tests checked for can
    // no longer be expressed in the type system. The single source of
    // truth for media defs is now the registry.

    // =========================================================================
    // validate_cap_args RULE tests (TEST578-TEST590)
    // =========================================================================

    fn make_test_cap_with_args(args: Vec<CapArg>) -> Cap {
        // Uses in=media:void — only for tests where no arg has a stdin source.
        let urn = CapUrn::from_string(&test_urn("test")).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);
        for arg in args {
            cap.add_arg(arg);
        }
        cap
    }

    fn make_test_cap_with_stdin_args(args: Vec<CapArg>) -> Cap {
        // Uses in=media:enc=utf-8 — for tests where at least one arg has a stdin source.
        let urn =
            CapUrn::from_string(r#"cap:in="media:enc=utf-8";test;out="media:record""#).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);
        for arg in args {
            cap.add_arg(arg);
        }
        cap
    }

    // TEST578: RULE1 - duplicate media_urns rejected
    #[test]
    fn test578_rule1_duplicate_media_urns() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(
                MEDIA_STRING, "arg0",
                true,
                vec![ArgSource::Position { position: 0 }],
            ),
            CapArg::new(
                MEDIA_STRING, "arg1",
                true,
                vec![ArgSource::Position { position: 1 }],
            ),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE1"), "Error should mention RULE1: {}", err);
    }

    // TEST579: RULE2 - empty sources rejected
    #[test]
    fn test579_rule2_empty_sources() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(MEDIA_STRING, "option", true, vec![]), // empty sources
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE2"), "Error should mention RULE2: {}", err);
    }

    // TEST580: RULE3 - two args that declare a stdin source are rejected
    #[test]
    fn test580_rule3_two_stdin_args_rejected() {
        let cap = make_test_cap_with_stdin_args(vec![
            CapArg::new(
                MEDIA_STRING, "input",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:enc=utf-8".to_string(),
                }],
            ),
            CapArg::new(
                MEDIA_INTEGER, "other",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:".to_string(),
                }],
            ),
        ]);
        let err = format!("{}", validate_cap_args(&cap).unwrap_err());
        assert!(err.contains("RULE3"), "Error should mention RULE3: {}", err);
        // Both arguments are named, by the URNs that identify them.
        assert!(err.contains(MEDIA_STRING) && err.contains(MEDIA_INTEGER), "{}", err);
    }

    // TEST581: RULE3 - two stdin args are rejected even when their stdin URNs are identical
    //
    // The rule used to allow this ("multiple stdin sources with the same URN is OK"), on the
    // reasoning that one stdin stream can only be one type. A cap has one main input: two args
    // that both claim it leave nothing to say which of them a piped value is.
    #[test]
    fn test581_rule3_identical_stdin_urns_are_still_two_main_inputs() {
        let cap = make_test_cap_with_stdin_args(vec![
            CapArg::new(
                MEDIA_STRING, "input",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:enc=utf-8".to_string(),
                }],
            ),
            CapArg::new(
                MEDIA_INTEGER, "other",
                true,
                vec![ArgSource::Stdin {
                    stdin: "media:enc=utf-8".to_string(),
                }],
            ),
        ]);
        let err = format!("{}", validate_cap_args(&cap).unwrap_err());
        assert!(err.contains("RULE3"), "Error should mention RULE3: {}", err);
    }

    // TEST582: RULE4 - duplicate source type in single arg rejected
    #[test]
    fn test582_rule4_duplicate_source_type() {
        let cap = make_test_cap_with_args(vec![CapArg::new(
            MEDIA_STRING, "arg0",
            true,
            vec![
                ArgSource::Position { position: 0 },
                ArgSource::Position { position: 1 }, // same source type twice
            ],
        )]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE4"), "Error should mention RULE4: {}", err);
    }

    // TEST583: RULE5 - duplicate position across args rejected
    #[test]
    fn test583_rule5_duplicate_position() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(
                MEDIA_STRING, "arg0",
                true,
                vec![ArgSource::Position { position: 0 }],
            ),
            CapArg::new(
                MEDIA_INTEGER, "arg0-2",
                true,
                vec![ArgSource::Position { position: 0 }],
            ),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE5"), "Error should mention RULE5: {}", err);
    }

    // TEST584: RULE6 - position gap rejected (0, 2 without 1)
    #[test]
    fn test584_rule6_position_gap() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(
                MEDIA_STRING, "arg0",
                true,
                vec![ArgSource::Position { position: 0 }],
            ),
            CapArg::new(
                MEDIA_INTEGER, "arg2",
                true,
                vec![ArgSource::Position { position: 2 }],
            ),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE6"), "Error should mention RULE6: {}", err);
    }

    // TEST585: RULE6 - sequential positions (0, 1, 2) pass
    #[test]
    fn test585_rule6_sequential_ok() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(
                MEDIA_STRING, "arg0",
                true,
                vec![ArgSource::Position { position: 0 }],
            ),
            CapArg::new(
                MEDIA_INTEGER, "arg1",
                true,
                vec![ArgSource::Position { position: 1 }],
            ),
        ]);
        let result = validate_cap_args(&cap);
        assert!(
            result.is_ok(),
            "Sequential positions should pass: {:?}",
            result.err()
        );
    }

    // TEST586: RULE7 - arg with both position and cli_flag rejected
    #[test]
    fn test586_rule7_position_and_cli_flag() {
        let cap = make_test_cap_with_args(vec![CapArg::new(
            MEDIA_STRING, "file",
            true,
            vec![
                ArgSource::Position { position: 0 },
                ArgSource::CliFlag {
                    cli_flag: "--file".to_string(),
                },
            ],
        )]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE7"), "Error should mention RULE7: {}", err);
    }

    // TEST587: RULE9 - duplicate cli_flag across args rejected
    #[test]
    fn test587_rule9_duplicate_cli_flag() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(
                MEDIA_STRING, "file",
                true,
                vec![ArgSource::CliFlag {
                    cli_flag: "--file".to_string(),
                }],
            ),
            CapArg::new(
                MEDIA_INTEGER, "file-2",
                true,
                vec![ArgSource::CliFlag {
                    cli_flag: "--file".to_string(),
                }],
            ),
        ]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("RULE9"), "Error should mention RULE9: {}", err);
    }

    // TEST588: RULE10 - reserved cli_flags rejected
    #[test]
    fn test588_rule10_reserved_cli_flags() {
        for &reserved in RESERVED_CLI_FLAGS {
            let cap = make_test_cap_with_args(vec![CapArg::new(
                MEDIA_STRING, "option",
                true,
                vec![ArgSource::CliFlag {
                    cli_flag: reserved.to_string(),
                }],
            )]);
            let result = validate_cap_args(&cap);
            assert!(
                result.is_err(),
                "Reserved flag '{}' should be rejected",
                reserved
            );
            let err = format!("{}", result.unwrap_err());
            assert!(
                err.contains("RULE10"),
                "Error for '{}' should mention RULE10: {}",
                reserved,
                err
            );
        }
    }

    // TEST589: valid cap args with mixed sources pass all rules
    #[test]
    fn test589_all_rules_pass() {
        let cap = make_test_cap_with_stdin_args(vec![
            CapArg::new(
                MEDIA_STRING, "input",
                true,
                vec![
                    ArgSource::Position { position: 0 },
                    ArgSource::Stdin {
                        stdin: "media:enc=utf-8".to_string(),
                    },
                ],
            ),
            CapArg::new(
                MEDIA_INTEGER, "arg1",
                false,
                vec![ArgSource::Position { position: 1 }],
            ),
        ]);
        let result = validate_cap_args(&cap);
        assert!(
            result.is_ok(),
            "Valid cap args should pass: {:?}",
            result.err()
        );
    }

    // TEST1294: RULE15 - void-input cap with stdin source rejected
    #[test]
    fn test1294_rule15_void_input_with_stdin_rejected() {
        let cap = make_test_cap_with_args(vec![CapArg::new(
            MEDIA_STRING, "input",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:enc=utf-8".to_string(),
            }],
        )]);
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("RULE15"),
            "Error should mention RULE15: {}",
            err
        );
    }

    // TEST1295: RULE15 - non-void-input cap without stdin source rejected
    #[test]
    fn test1295_rule15_non_void_input_without_stdin_rejected() {
        let urn =
            CapUrn::from_string(r#"cap:in="media:enc=utf-8";test;out="media:record""#).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);
        cap.add_arg(CapArg::new(
            MEDIA_STRING, "input",
            true,
            vec![ArgSource::CliFlag {
                cli_flag: "--input".to_string(),
            }],
        ));
        let result = validate_cap_args(&cap);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("RULE15"),
            "Error should mention RULE15: {}",
            err
        );
    }

    // TEST1296: RULE15 - void-input cap with only cli_flag sources passes
    #[test]
    fn test1296_rule15_void_input_cli_only_ok() {
        let cap = make_test_cap_with_args(vec![CapArg::new(
            MEDIA_STRING, "input",
            true,
            vec![ArgSource::CliFlag {
                cli_flag: "--input".to_string(),
            }],
        )]);
        let result = validate_cap_args(&cap);
        assert!(
            result.is_ok(),
            "Void-input cap with only CLI sources should pass: {:?}",
            result.err()
        );
    }

    // TEST1297: RULE15 - non-void-input cap with stdin source passes
    #[test]
    fn test1297_rule15_non_void_input_with_stdin_ok() {
        let cap = make_test_cap_with_stdin_args(vec![CapArg::new(
            MEDIA_STRING, "input",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:enc=utf-8".to_string(),
            }],
        )]);
        let result = validate_cap_args(&cap);
        assert!(
            result.is_ok(),
            "Non-void-input cap with stdin should pass: {:?}",
            result.err()
        );
    }

    // TEST12705: RULE15 - the stdin argument's URN is the cap's `in=`, as a
    // tagged URN: the same tags in another order pass, another type does not.
    #[test]
    fn test12705_rule15_the_stdin_argument_is_the_caps_input() {
        let cap_of = |stdin: &str| {
            let urn = CapUrn::from_string(
                r#"cap:in="media:document;ext=pdf";test;out="media:record""#,
            )
            .unwrap();
            let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);
            cap.add_arg(CapArg::new(
                "media:enc=utf-8;file-path", "document",
                true,
                vec![ArgSource::Stdin { stdin: stdin.to_string() }],
            ));
            cap
        };
        assert!(validate_cap_args(&cap_of("media:document;ext=pdf")).is_ok());
        assert!(
            validate_cap_args(&cap_of("media:ext=pdf;document")).is_ok(),
            "tag order is not identity"
        );
        let err = format!("{}", validate_cap_args(&cap_of("media:document")).unwrap_err());
        assert!(err.contains("RULE15") && err.contains("media:document;ext=pdf"), "{}", err);
    }

    // TEST12706: RULE17 - every argument has a well-formed name, unique in its cap.
    #[test]
    fn test12706_rule17_argument_names() {
        let flag = |urn: &str, name: &str, flag: &str| {
            CapArg::new(urn, name, false, vec![ArgSource::CliFlag { cli_flag: flag.to_string() }])
        };
        // Two args, two names: fine. A name says nothing of the URN it labels.
        let cap = make_test_cap_with_args(vec![
            flag(MEDIA_STRING, "window", "--a"),
            flag(MEDIA_INTEGER, "benchmark", "--b"),
        ]);
        assert!(validate_cap_args(&cap).is_ok(), "{:?}", validate_cap_args(&cap).err());

        let refused = |args: Vec<CapArg>| format!("{}", validate_cap_args(&make_test_cap_with_args(args)).unwrap_err());
        let err = refused(vec![flag(MEDIA_STRING, "", "--a")]);
        assert!(err.contains("RULE17") && err.contains("has no name"), "{}", err);
        for bad in ["9lives", "has space", "dotted.name", "a=b", "true", "false", "null", "-lead"] {
            let err = refused(vec![flag(MEDIA_STRING, bad, "--a")]);
            assert!(err.contains("RULE17") && err.contains("not a valid name"), "{bad}: {err}");
        }
        for good in ["x", "_x", "model-spec", "top_k", "A1"] {
            let cap = make_test_cap_with_args(vec![flag(MEDIA_STRING, good, "--a")]);
            assert!(validate_cap_args(&cap).is_ok(), "{good}: {:?}", validate_cap_args(&cap).err());
        }
        let err = refused(vec![flag(MEDIA_STRING, "same", "--a"), flag(MEDIA_INTEGER, "same", "--b")]);
        assert!(err.contains("RULE17") && err.contains("share the name 'same'"), "{}", err);
        assert!(err.contains(MEDIA_STRING) && err.contains(MEDIA_INTEGER), "both args are named by URN: {}", err);
    }

    // TEST12707: RULE18 - an argument says what it accepts in one place; and what
    // it accepts is not what it is delivered under.
    #[test]
    fn test12707_rule18_accepts_and_the_two_urns_of_an_argument() {
        let series = "media:fmt=json;list;record;time-series";
        let benchmark = CapArg::new(
            "media:benchmark;fmt=json;list;record;time-series", "benchmark",
            true,
            vec![ArgSource::CliFlag { cli_flag: "--benchmark".to_string() }],
        )
        .accepting(series);
        // It accepts a plain series, and is delivered under its own URN: two
        // args accepting one type stay distinct on the wire.
        assert_eq!(benchmark.accepted_urn(), series);
        assert_eq!(benchmark.stream_urn(), "media:benchmark;fmt=json;list;record;time-series");

        // The main input accepts, and is delivered under, its stdin URN.
        let main = CapArg::new(
            "media:enc=utf-8;file-path", "path",
            true,
            vec![ArgSource::Stdin { stdin: series.to_string() }],
        );
        assert_eq!(main.accepted_urn(), series);
        assert_eq!(main.stream_urn(), series);

        // An arg with neither accepts its own URN.
        let plain = CapArg::new(MEDIA_INTEGER, "window", false, vec![ArgSource::CliFlag { cli_flag: "--window".to_string() }]);
        assert_eq!(plain.accepted_urn(), MEDIA_INTEGER);
        assert_eq!(plain.stream_urn(), MEDIA_INTEGER);

        let urn = CapUrn::from_string(&format!(r#"cap:in="{series}";compare;out="media:record""#)).unwrap();
        let mut cap = Cap::new(urn.clone(), "Compare".to_string(), vec!["compare".to_string()]);
        cap.add_arg(main.clone());
        cap.add_arg(benchmark);
        assert!(validate_cap_args(&cap).is_ok(), "{:?}", validate_cap_args(&cap).err());

        // `accepts` beside a stdin source says it twice.
        let mut twice = Cap::new(urn, "Compare".to_string(), vec!["compare".to_string()]);
        twice.add_arg(main.accepting(series));
        let err = format!("{}", validate_cap_args(&twice).unwrap_err());
        assert!(err.contains("RULE18") && err.contains("media:enc=utf-8;file-path"), "{}", err);
    }

    fn cap_with_outputs(out_spec: &str, outputs: Vec<CapOutput>) -> Cap {
        let urn = CapUrn::from_string(&format!(r#"cap:in="media:enc=utf-8";test;out="{out_spec}""#)).unwrap();
        let mut cap = Cap::new(urn, "Test".to_string(), vec!["cmd".to_string()]);
        cap.set_outputs(outputs);
        cap
    }

    // TEST12708: RULE19, RULE20 - outputs have well-formed, distinct names and
    // distinct media URNs, compared as URNs.
    #[test]
    fn test12708_rule19_rule20_output_names_and_identities() {
        let fit = || CapOutput::new("media:fmt=json;record;regression", "fit", "the fit");
        let ok = cap_with_outputs(
            "media:fmt=json;record;regression",
            vec![fit(), CapOutput::new("media:fmt=json;list;record;residual", "residuals", "")],
        );
        assert!(validate_cap_outputs(&ok).is_ok(), "{:?}", validate_cap_outputs(&ok).err());

        let refused = |outputs: Vec<CapOutput>| {
            format!("{}", validate_cap_outputs(&cap_with_outputs("media:fmt=json;record;regression", outputs)).unwrap_err())
        };
        let err = refused(vec![fit(), CapOutput::new("media:fmt=json;list;record;residual", "", "")]);
        assert!(err.contains("RULE19") && err.contains("has no name"), "{}", err);
        let err = refused(vec![fit(), CapOutput::new("media:fmt=json;list;record;residual", "null", "")]);
        assert!(err.contains("RULE19") && err.contains("not a valid name"), "{}", err);
        let err = refused(vec![fit(), CapOutput::new("media:fmt=json;list;record;residual", "fit", "")]);
        assert!(err.contains("RULE19") && err.contains("share the name 'fit'"), "{}", err);
        // The same URN with its tags in another order is the same output.
        let err = refused(vec![fit(), CapOutput::new("media:regression;record;fmt=json", "again", "")]);
        assert!(err.contains("RULE20"), "{}", err);
    }

    // TEST12709: RULE21 - exactly one output is the main one, the cap's `out=`;
    // a cap whose `out=` is void declares none.
    #[test]
    fn test12709_rule21_exactly_one_main_output() {
        let fit = "media:fmt=json;record;regression";
        let none = cap_with_outputs(fit, vec![CapOutput::new("media:fmt=json;list;record;residual", "residuals", "")]);
        let err = format!("{}", validate_cap_outputs(&none).unwrap_err());
        assert!(err.contains("RULE21") && err.contains("0 outputs"), "{}", err);
        let bare = cap_with_outputs(fit, vec![]);
        assert!(format!("{}", validate_cap_outputs(&bare).unwrap_err()).contains("RULE21"));

        let one = cap_with_outputs(fit, vec![CapOutput::new("media:regression;fmt=json;record", "fit", "")]);
        assert!(validate_cap_outputs(&one).is_ok(), "the main output is found by URN equivalence");
        assert_eq!(one.main_output().map(|o| o.name.as_str()), Some("fit"));

        let void = cap_with_outputs("media:void", vec![]);
        assert!(validate_cap_outputs(&void).is_ok());
        assert!(void.main_output().is_none());
        let void_with = cap_with_outputs("media:void", vec![CapOutput::new("media:enc=utf-8", "text", "")]);
        let err = format!("{}", validate_cap_outputs(&void_with).unwrap_err());
        assert!(err.contains("RULE21") && err.contains("media:void"), "{}", err);
    }

    // TEST12710: RULE22 - only a non-main output of a cap that takes an input
    // may carry that input's type.
    #[test]
    fn test12710_rule22_what_an_output_carries() {
        let router = cap_with_outputs(
            "media:enc=utf-8;kept",
            vec![
                CapOutput::new("media:enc=utf-8;kept", "yes", "").optional(true),
                CapOutput::new("media:enc=utf-8;dropped", "no", "").optional(true).carrying_input(),
            ],
        );
        assert!(validate_cap_outputs(&router).is_ok(), "{:?}", validate_cap_outputs(&router).err());

        let main_carries = cap_with_outputs(
            "media:enc=utf-8;kept",
            vec![CapOutput::new("media:enc=utf-8;kept", "yes", "").carrying_input()],
        );
        let err = format!("{}", validate_cap_outputs(&main_carries).unwrap_err());
        assert!(err.contains("RULE22") && err.contains("main output"), "{}", err);

        let urn = CapUrn::from_string(r#"cap:in="media:void";now;out="media:enc=utf-8;stamp""#).unwrap();
        let mut source = Cap::new(urn, "Now".to_string(), vec!["now".to_string()]);
        source.set_outputs(vec![
            CapOutput::new("media:enc=utf-8;stamp", "stamp", ""),
            CapOutput::new("media:enc=utf-8;echo", "echo", "").carrying_input(),
        ]);
        let err = format!("{}", validate_cap_outputs(&source).unwrap_err());
        assert!(err.contains("RULE22") && err.contains("takes none"), "{}", err);
    }

    // TEST1953: RULE14 — `streaming: true` is accepted on the main input (the
    // stdin arg equivalent to `in=`) and refused on any other argument: a
    // side option has no wire stream, so it has nothing to consume
    // incrementally, and the rule keeps the hop rule one-dimensional.
    #[test]
    fn test1953_rule14_streaming_only_on_main_input() {
        let main = CapArg::new(
            "media:enc=utf-8", "input",
            true,
            vec![ArgSource::Stdin {
                stdin: "media:enc=utf-8".to_string(),
            }],
        )
        .streaming(true);
        let ok = make_test_cap_with_stdin_args(vec![main.clone()]);
        assert!(
            validate_cap_args(&ok).is_ok(),
            "streaming main input must pass: {:?}",
            validate_cap_args(&ok).err()
        );
        assert_eq!(ok.streaming_shape(), (true, false));

        let side = CapArg::new(
            MEDIA_INTEGER, "count",
            false,
            vec![ArgSource::CliFlag {
                cli_flag: "--count".to_string(),
            }],
        )
        .streaming(true);
        let bad = make_test_cap_with_stdin_args(vec![main.streaming(false), side]);
        let err = validate_cap_args(&bad).expect_err("streaming side option must be refused");
        let text = err.to_string();
        assert!(text.contains("RULE14"), "error names RULE14: {text}");
        assert!(text.contains(MEDIA_INTEGER), "error names the offending arg: {text}");
    }

    // TEST590: validate_cap_args accepts cap with only cli_flag sources (no positions)
    #[test]
    fn test590_cli_flag_only_args() {
        let cap = make_test_cap_with_args(vec![
            CapArg::new(
                MEDIA_STRING, "input",
                true,
                vec![ArgSource::CliFlag {
                    cli_flag: "--input".to_string(),
                }],
            ),
            CapArg::new(
                MEDIA_INTEGER, "count",
                false,
                vec![ArgSource::CliFlag {
                    cli_flag: "--count".to_string(),
                }],
            ),
        ]);
        let result = validate_cap_args(&cap);
        assert!(
            result.is_ok(),
            "CLI-flag-only args should pass: {:?}",
            result.err()
        );
    }
}

/// Validate cap arguments against canonical definition
pub async fn validate_cap_arguments(
    registry: &crate::cap::registry::FabricRegistry,
    schema_registry: Arc<ProfileSchemaRegistry>,
    fabric_registry: Arc<crate::FabricRegistry>,
    cap_urn: &str,
    arguments: &[Value],
) -> Result<(), ValidationError> {
    let canonical_cap =
        registry
            .get_cap(cap_urn)
            .await
            .map_err(|_| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;
    let validator = InputValidator::new(schema_registry, fabric_registry);
    validator
        .validate_positional_arguments(&canonical_cap, arguments)
        .await
}

/// Validate cap output against canonical definition
pub async fn validate_cap_output(
    registry: &crate::cap::registry::FabricRegistry,
    schema_registry: Arc<ProfileSchemaRegistry>,
    fabric_registry: Arc<crate::FabricRegistry>,
    cap_urn: &str,
    output: &Value,
) -> Result<(), ValidationError> {
    let canonical_cap =
        registry
            .get_cap(cap_urn)
            .await
            .map_err(|_| ValidationError::UnknownCap {
                cap_urn: cap_urn.to_string(),
            })?;
    let validator = OutputValidator::new(schema_registry, fabric_registry);
    validator.validate_output(&canonical_cap, output).await
}

/// Validate that a local cap matches its canonical definition
pub async fn validate_cap_canonical(
    registry: &crate::cap::registry::FabricRegistry,
    cap: &Cap,
) -> Result<(), ValidationError> {
    registry
        .validate_cap(cap)
        .await
        .map_err(|e| ValidationError::InvalidCapSchema {
            cap_urn: cap.urn_string(),
            issue: e.to_string(),
        })
}

/// Validate that all media URN references in a cap definition are resolvable
///
/// This function checks:
/// - The 'in' tag media URN (unless '*')
/// - The 'out' tag media URN (unless '*')
/// - Every argument's media_urn
/// - Every output's media_urn
///
/// Resolution order:
/// 1. Cap's local media_defs table (cap-specific overrides)
/// 2. Registry's in-memory + disk cache
/// 3. Online registry fetch (blocked by the registry's offline flag if set)
pub async fn validate_media_urn_references(
    cap: &Cap,
    registry: &crate::media::registry::FabricRegistry,
) -> Result<(), ValidationError> {
    let cap_urn = cap.urn_string();

    // Collect all media URNs to validate
    let mut urns_to_check: Vec<(String, String)> = Vec::new();

    // Check in/out tags from the cap URN
    let in_spec = cap.urn.in_spec();
    if in_spec != "*" {
        urns_to_check.push(("in tag".to_string(), in_spec.to_string()));
    }

    let out_spec = cap.urn.out_spec();
    if out_spec != "*" {
        urns_to_check.push(("out tag".to_string(), out_spec.to_string()));
    }

    // Check all argument media URNs
    for arg in cap.get_args() {
        urns_to_check.push((
            format!("argument '{}'", arg.media_urn),
            arg.media_urn.clone(),
        ));
    }

    // Check output media URN
    if let Some(output) = cap.main_output() {
        urns_to_check.push(("output".to_string(), output.media_urn.clone()));
    }

    // Validate each media URN using the single resolution path
    for (location, media_urn) in urns_to_check {
        if let Err(_) = crate::media::spec::resolve_media_urn(&media_urn, registry).await {
            return Err(ValidationError::UnresolvableMediaUrn {
                cap_urn: cap_urn.clone(),
                media_urn,
                location: location.to_string(),
            });
        }
    }

    Ok(())
}
