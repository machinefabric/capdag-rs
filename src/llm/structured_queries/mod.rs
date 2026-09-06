//! Formalized Structured Output Questions
//!
//! This module defines a standardized approach to structured LLM questions
//! with predefined schemas, prompts, and expected outputs for consistent
//! and reliable AI-powered document processing.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use tera::{Context, Tera};

pub mod registry;
pub use registry::StructuredQueryRegistry;

/// Represents a structured output question type with schema and prompt template
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredQuery {
    /// Unique identifier for this query type
    pub name: String,

    /// Human-readable description of what this query does
    pub description: String,

    /// Prompt template with placeholders for dynamic content
    /// Use {content} for the main text, {question} for user questions, etc.
    pub prompt_template: String,

    /// JSON schema defining the expected output structure
    /// Can be either a static schema or template-rendered schema
    pub output_schema: JsonValue,

    /// Optional schema template for dynamic schemas
    /// If present, this will be used to render dynamic schemas with variables
    pub schema_template: Option<String>,

    /// Optional metadata about the query
    pub metadata: HashMap<String, String>,
}

/// Query definition from JSON configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueryDefinition {
    name: String,
    description: String,
    prompt_file: String,
    schema_file: String,
    metadata: Option<HashMap<String, String>>,
}

impl StructuredQuery {
    /// Create a new structured query
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        prompt_template: impl Into<String>,
        output_schema: JsonValue,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            prompt_template: prompt_template.into(),
            output_schema,
            schema_template: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new structured query with a schema template
    pub fn new_with_schema_template(
        name: impl Into<String>,
        description: impl Into<String>,
        prompt_template: impl Into<String>,
        output_schema: JsonValue,
        schema_template: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            prompt_template: prompt_template.into(),
            output_schema,
            schema_template: Some(schema_template.into()),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the query
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Generate the actual prompt using Tera templating
    pub fn generate_prompt(
        &self,
        substitutions: &HashMap<String, serde_json::Value>,
    ) -> Result<String> {
        let mut tera = Tera::new("templates/**/*").unwrap_or_else(|_| Tera::new("").unwrap());

        // Add the template to Tera with a unique name
        let template_name = format!("query_{}", self.name);
        if let Err(e) = tera.add_raw_template(&template_name, &self.prompt_template) {
            return Err(anyhow::anyhow!(
                "TEMPLATE_PARSE_ERROR: Failed to parse template for query {}: {}",
                self.name,
                e
            ));
        }

        // Create Tera context from substitutions
        let mut context = Context::new();
        for (key, value) in substitutions {
            context.insert(key, value);
        }

        // Render the template
        tera.render(&template_name, &context).map_err(|e| {
            anyhow::anyhow!(
                "TEMPLATE_RENDER_ERROR: Failed to render template for query {}: {}",
                self.name,
                e
            )
        })
    }

    /// Generate dynamic schema using Tera templating if schema template is available
    /// Falls back to static schema if no template is provided
    pub fn generate_schema(
        &self,
        schema_variables: &HashMap<String, serde_json::Value>,
    ) -> Result<JsonValue> {
        if let Some(ref schema_template) = self.schema_template {
            // Use Tera to render the schema template
            let mut tera = Tera::new("templates/**/*").unwrap_or_else(|_| Tera::new("").unwrap());

            // Add the schema template to Tera with a unique name
            let template_name = format!("schema_{}", self.name);
            if let Err(e) = tera.add_raw_template(&template_name, schema_template) {
                return Err(anyhow::anyhow!(
                    "SCHEMA_TEMPLATE_PARSE_ERROR: Failed to parse schema template for query {}: {}",
                    self.name,
                    e
                ));
            }

            // Create Tera context from schema variables
            let mut context = Context::new();
            for (key, value) in schema_variables {
                context.insert(key, value);
            }

            // Render the schema template
            let rendered_schema = tera.render(&template_name, &context).map_err(|e| {
                anyhow::anyhow!(
                    "SCHEMA_TEMPLATE_RENDER_ERROR: Failed to render schema template for query {}: {}",
                    self.name,
                    e
                )
            })?;

            // Parse the rendered schema as JSON
            serde_json::from_str(&rendered_schema).map_err(|e| {
                anyhow::anyhow!(
                    "SCHEMA_JSON_PARSE_ERROR: Failed to parse rendered schema as JSON for query {}: {}",
                    self.name,
                    e
                )
            })
        } else {
            // Return static schema
            Ok(self.output_schema.clone())
        }
    }

    /// Check if this query has a dynamic schema template
    pub fn has_schema_template(&self) -> bool {
        self.schema_template.is_some()
    }

    /// Validate that the provided JSON matches the expected schema
    pub fn validate_output(&self, output: &JsonValue) -> Result<bool> {
        // Basic validation - in production you'd want a full JSON schema validator
        match (&self.output_schema, output) {
            (JsonValue::Object(schema), JsonValue::Object(data)) => {
                if let Some(JsonValue::Object(_properties)) = schema.get("properties") {
                    if let Some(JsonValue::Array(required)) = schema.get("required") {
                        // Check all required fields are present
                        for required_field in required {
                            if let JsonValue::String(field_name) = required_field {
                                if !data.contains_key(field_name) {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Builder for creating custom structured queries
pub struct StructuredQueryBuilder {
    name: Option<String>,
    description: Option<String>,
    prompt_template: Option<String>,
    output_schema: Option<JsonValue>,
    schema_template: Option<String>,
    metadata: HashMap<String, String>,
}

impl StructuredQueryBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            name: None,
            description: None,
            prompt_template: None,
            output_schema: None,
            schema_template: None,
            metadata: HashMap::new(),
        }
    }

    /// Set the query name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the query description
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the prompt template
    pub fn prompt_template(mut self, template: impl Into<String>) -> Self {
        self.prompt_template = Some(template.into());
        self
    }

    /// Set the output schema
    pub fn output_schema(mut self, schema: JsonValue) -> Self {
        self.output_schema = Some(schema);
        self
    }

    /// Set the schema template for dynamic schemas
    pub fn schema_template(mut self, template: impl Into<String>) -> Self {
        self.schema_template = Some(template.into());
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Build the structured query
    pub fn build(self) -> Result<StructuredQuery> {
        let name = self
            .name
            .ok_or_else(|| anyhow::anyhow!("MISSING_QUERY_NAME: Query name is required"))?;

        let description = self
            .description
            .ok_or_else(|| anyhow::anyhow!("MISSING_QUERY_DESCRIPTION: Query description is required"))?;

        let prompt_template = self
            .prompt_template
            .ok_or_else(|| anyhow::anyhow!("MISSING_PROMPT_TEMPLATE: Prompt template is required"))?;

        let output_schema = self
            .output_schema
            .ok_or_else(|| anyhow::anyhow!("MISSING_OUTPUT_SCHEMA: Output schema is required"))?;

        let mut query = if let Some(schema_template) = self.schema_template {
            StructuredQuery::new_with_schema_template(
                name,
                description,
                prompt_template,
                output_schema,
                schema_template,
            )
        } else {
            StructuredQuery::new(name, description, prompt_template, output_schema)
        };
        query.metadata = self.metadata;

        Ok(query)
    }
}

impl Default for StructuredQueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Schema progress tracker for constrained generation
#[derive(Debug, Clone)]
pub struct SchemaProgressTracker {
    /// Total fields in the schema (required + optional)
    pub total_fields: u32,
    /// Required fields that must be completed
    pub required_fields: Vec<String>,
    /// Optional fields
    pub optional_fields: Vec<String>,
    /// Currently completed fields
    pub completed_fields: HashSet<String>,
}

impl SchemaProgressTracker {
    /// Create tracker from JSON schema
    pub fn from_json_schema(schema: &serde_json::Value) -> Self {
        let mut required_fields = Vec::new();
        let mut optional_fields = Vec::new();

        // Parse required fields
        if let Some(required) = schema.get("required").and_then(|r| r.as_array()) {
            for field in required {
                if let Some(field_name) = field.as_str() {
                    required_fields.push(field_name.to_string());
                }
            }
        }

        // Parse all properties to find optional fields
        if let Some(properties) = schema.get("properties").and_then(|p| p.as_object()) {
            for field_name in properties.keys() {
                if !required_fields.contains(field_name) {
                    optional_fields.push(field_name.clone());
                }
            }
        }

        let total_fields = (required_fields.len() + optional_fields.len()) as u32;

        Self {
            total_fields,
            required_fields,
            optional_fields,
            completed_fields: std::collections::HashSet::new(),
        }
    }

    /// Update progress by counting field names in partial JSON
    pub fn update_from_partial_json(&mut self, partial_json: &str) -> f32 {
        // Don't clear completed fields - we want to track cumulative progress
        // Just re-count from scratch each time
        let mut found_fields = HashSet::new();

        // Simple approach: search for field names in quotes
        self.count_fields_in_text_into(partial_json, &mut found_fields);

        // Update our tracked fields
        self.completed_fields = found_fields;

        // Calculate progress percentage
        if self.total_fields == 0 {
            1.0
        } else {
            self.completed_fields.len() as f32 / self.total_fields as f32
        }
    }

    /// Count fields by searching for "field_name": patterns in the text
    fn count_fields_in_text_into(&self, text: &str, found_fields: &mut HashSet<String>) {
        // Combine all field names to search for
        let all_fields: Vec<&String> = self
            .required_fields
            .iter()
            .chain(self.optional_fields.iter())
            .collect();

        for field_name in all_fields {
            // Look for the pattern "field_name": (with quotes and colon)
            let pattern = format!("\"{}\":", field_name);
            if text.contains(&pattern) {
                // Additional check: make sure there's some content after the colon
                if let Some(pos) = text.find(&pattern) {
                    let after_field = &text[pos + pattern.len()..];
                    // Skip whitespace and see if there's actual content
                    let trimmed = after_field.trim_start();
                    if !trimmed.is_empty()
                        && !trimmed.starts_with("null")
                        && !trimmed.starts_with("\"\"")
                    {
                        found_fields.insert(field_name.clone());
                    }
                }
            }
        }
    }

    /// Get current progress (0.0 to 1.0)
    pub fn progress(&self) -> f32 {
        if self.total_fields == 0 {
            1.0
        } else {
            self.completed_fields.len() as f32 / self.total_fields as f32
        }
    }
}

// Bit choice result — carries the full judgment envelope
// (semantic-primitives law P2): the decision plus the model's
// confidence and a one-sentence rationale.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MakeDecisionResult {
    /// Binary answer: 0 for No/False, 1 for Yes/True
    pub res: i32,
    /// Model confidence, 0.0–1.0 (schema-constrained)
    pub confidence: f32,
    /// One-sentence rationale naming the decisive evidence
    pub reason: String,
}

/// One per-question decision inside a multiple-decisions result.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DecisionItem {
    /// Binary answer: 0 for No/False, 1 for Yes/True
    pub res: i32,
    /// Model confidence, 0.0–1.0 (schema-constrained)
    pub confidence: f32,
    /// One-sentence rationale naming the decisive evidence
    pub reason: String,
}

/// Multiple Make Multiple Decisions result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MakeMultipleDecisionsResult {
    /// Results for each Make Decision question, in question order
    pub res: Vec<DecisionItem>,
}

/// Question answering result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QaResult {
    /// Answer to the question
    pub answer: String,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f32,
    /// Relevant context from the document
    pub context: String,
    /// Source information
    pub source: String,
}

/// Yes/No binary question result
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct YesNoResult {
    /// Binary answer: "yes" or "no"
    pub answer: String,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f32,
    /// Reasoning for the answer
    pub reasoning: Option<String>,
    /// Relevant context from the document
    pub context: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test basic StructuredQuery creation with name, description, prompt template, and schema
    /// Validates that all fields are properly initialized and accessible
    #[test]
    fn test0229_structured_query_creation() {
        let query = StructuredQuery::new(
            "test_query",
            "A test query",
            "Analyze: {content}",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "result": {"type": "string"}
                },
                "required": ["result"]
            }),
        );

        assert_eq!(query.name, "test_query");
        assert_eq!(query.description, "A test query");
        assert_eq!(query.prompt_template, "Analyze: {content}");
    }

    /// Every prose-output query bounds its free-text string field with a `maxLength`,
    /// so grammar-constrained generation always closes the JSON object instead of being
    /// truncated mid-string when the token budget is reached. This is the schema-side
    /// half of the summarize/ask truncation fix — without the bound the model can run a
    /// field to the token cap, leaving an unterminated JSON that fails to parse.
    #[test]
    fn test0231_prose_schemas_bound_their_text_fields() {
        let registry = StructuredQueryRegistry::new();
        for (query_name, field) in [("semantic_summarize", "summary"), ("semantic_ask", "answer")] {
            let query = registry
                .query(query_name)
                .unwrap_or_else(|e| panic!("query {query_name}: {e}"));
            let max_len = query.output_schema["properties"][field]["maxLength"].as_u64();
            assert!(
                matches!(max_len, Some(n) if n > 0),
                "{query_name}.{field} must declare a positive maxLength so constrained \
                 generation completes the JSON; got {:?}",
                query.output_schema["properties"][field].get("maxLength"),
            );
        }
    }

    /// Test Tera template rendering with variable substitution
    /// Validates that prompt templates render correctly with provided substitutions
    #[test]
    fn test0230_prompt_generation() {
        let query = StructuredQuery::new(
            "test",
            "Test",
            "Process {{ content }} with {{ model }}",
            serde_json::json!({"type": "object"}),
        );

        let mut substitutions: HashMap<String, serde_json::Value> = HashMap::new();
        substitutions.insert(
            "content".to_string(),
            serde_json::Value::String("sample text".to_string()),
        );
        substitutions.insert(
            "model".to_string(),
            serde_json::Value::String("gpt-4".to_string()),
        );

        let prompt = query.generate_prompt(&substitutions).unwrap();
        assert_eq!(prompt, "Process sample text with gpt-4");
    }

    /// Test StructuredQueryRegistry loading and query retrieval operations
    /// Validates that registry loads queries from embedded files and provides access
    #[test]
    fn test0228_registry_operations() {
        let registry = StructuredQueryRegistry::new();

        // Should have loaded queries from files or fallback
        assert!(!registry.is_empty());

        // Check query list
        let queries = registry.list_queries();
        assert!(!queries.is_empty());

        // Should be able to get a query
        if let Some(first_query_name) = queries.first() {
            assert!(registry.get(first_query_name).is_some());
        }
    }

    /// Test StructuredQueryBuilder pattern for fluent query creation
    /// Validates that builder pattern works correctly with method chaining and metadata
    #[test]
    fn test0232_query_builder() {
        let query = StructuredQueryBuilder::new()
            .name("custom_query")
            .description("Custom test query")
            .prompt_template("Custom prompt: {input}")
            .output_schema(serde_json::json!({"type": "object"}))
            .metadata("version", "1.0")
            .build()
            .unwrap();

        assert_eq!(query.name, "custom_query");
        assert_eq!(query.metadata.get("version"), Some(&"1.0".to_string()));
    }

    /// Test JSON schema validation against query outputs
    /// Validates that LLM outputs are properly validated against expected schemas
    #[test]
    fn test0233_output_validation() {
        let query = StructuredQuery::new(
            "test",
            "Test",
            "Test prompt",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "answer": {"type": "string"},
                    "confidence": {"type": "number"}
                },
                "required": ["answer"]
            }),
        );

        // Valid output
        let valid_output = serde_json::json!({
            "answer": "Test answer",
            "confidence": 0.9
        });
        assert!(query.validate_output(&valid_output).unwrap());

        // Invalid output (missing required field)
        let invalid_output = serde_json::json!({
            "confidence": 0.9
        });
        assert!(!query.validate_output(&invalid_output).unwrap());
    }

    /// Test specific make_decision query loading and validation
    /// Validates that binary choice queries work correctly with template rendering
    #[test]
    fn test0234_make_decision_query_type() {
        let registry = StructuredQueryRegistry::new();

        // Check that make_decision query is loaded
        let make_decision_query = registry.get("make_decision");
        assert!(make_decision_query.is_some());

        let query = make_decision_query.unwrap();
        assert_eq!(query.name, "make_decision");
        assert_eq!(
            query.description,
            "Answer boolean question with 0/1 responses"
        );
        assert_eq!(
            query.metadata.get("category"),
            Some(&"binary_qa".to_string())
        );

        // Test template rendering
        let mut substitutions: HashMap<String, serde_json::Value> = HashMap::new();
        substitutions.insert(
            "content".to_string(),
            serde_json::Value::String("The document discusses climate change.".to_string()),
        );
        substitutions.insert(
            "question".to_string(),
            serde_json::Value::String(
                "Does this document discuss environmental issues?".to_string(),
            ),
        );

        let prompt = query.generate_prompt(&substitutions).unwrap();
        assert!(prompt.contains("climate change"));
        assert!(prompt.contains("environmental issues"));
        assert!(prompt.contains("0") || prompt.contains("1")); // Template should mention 0/1

        // Validate schema structure
        let schema = &query.output_schema;
        assert!(schema.is_object());
        let properties = schema.as_object().unwrap().get("properties").unwrap();
        assert!(properties.as_object().unwrap().contains_key("res"));

        // Test valid output validation — the decision carries the full
        // judgment envelope (confidence + reason are REQUIRED since the
        // decide refit; a bare {"res"} is no longer a valid decision).
        let valid_output = serde_json::json!({
            "res": 1,
            "confidence": 0.9,
            "reason": "the content states it directly"
        });
        assert!(query.validate_output(&valid_output).unwrap());

        // Test another valid output
        let valid_output_zero = serde_json::json!({
            "res": 0,
            "confidence": 0.6,
            "reason": "no supporting statement found"
        });
        assert!(query.validate_output(&valid_output_zero).unwrap());

        // The OLD bare shape must now be invalid — the envelope fields
        // are required.
        let bare = serde_json::json!({ "res": 1 });
        assert!(!query.validate_output(&bare).unwrap());

        // Test invalid output validation (missing required field)
        let invalid_output = serde_json::json!({
            "answer": "maybe" // Wrong field name
        });
        assert!(!query.validate_output(&invalid_output).unwrap());
    }

    /// Test Tera template rendering for dynamic schema generation
    /// Validates that schemas can be generated dynamically with template variables
    #[test]
    fn test0235_dynamic_schema_generation() {
        // Create a query with a schema template
        let schema_template = r#"{
            "type": "object",
            "properties": {
                "choice": {
                    "type": "integer",
                    "minimum": {{ min_value }},
                    "maximum": {{ max_value }}
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0
                }
            },
            "required": ["choice", "confidence"]
        }"#;

        let query = StructuredQuery::new_with_schema_template(
            "dynamic_choice",
            "Make a choice within dynamic range",
            "Choose a number between {{ min_value }} and {{ max_value }}: {{ question }}",
            serde_json::json!({}), // Default schema (unused when template exists)
            schema_template,
        );

        assert!(query.has_schema_template());

        // Test schema generation with variables
        let mut schema_vars: HashMap<String, serde_json::Value> = HashMap::new();
        schema_vars.insert(
            "min_value".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1)),
        );
        schema_vars.insert(
            "max_value".to_string(),
            serde_json::Value::Number(serde_json::Number::from(10)),
        );

        let dynamic_schema = query.generate_schema(&schema_vars).unwrap();

        // Verify the schema was rendered correctly
        let properties = dynamic_schema
            .as_object()
            .unwrap()
            .get("properties")
            .unwrap();
        let choice_prop = properties.as_object().unwrap().get("choice").unwrap();
        assert_eq!(choice_prop.get("minimum").unwrap().as_i64().unwrap(), 1);
        assert_eq!(choice_prop.get("maximum").unwrap().as_i64().unwrap(), 10);

        // Test with different variables
        let mut schema_vars2: HashMap<String, serde_json::Value> = HashMap::new();
        schema_vars2.insert(
            "min_value".to_string(),
            serde_json::Value::Number(serde_json::Number::from(0)),
        );
        schema_vars2.insert(
            "max_value".to_string(),
            serde_json::Value::Number(serde_json::Number::from(100)),
        );

        let dynamic_schema2 = query.generate_schema(&schema_vars2).unwrap();
        let properties2 = dynamic_schema2
            .as_object()
            .unwrap()
            .get("properties")
            .unwrap();
        let choice_prop2 = properties2.as_object().unwrap().get("choice").unwrap();
        assert_eq!(choice_prop2.get("minimum").unwrap().as_i64().unwrap(), 0);
        assert_eq!(choice_prop2.get("maximum").unwrap().as_i64().unwrap(), 100);
    }

    /// Test fallback to static schema when no template exists
    /// Validates that queries without schema templates use their static schemas
    #[test]
    fn test0236_static_schema_fallback() {
        // Create a query without schema template
        let query = StructuredQuery::new(
            "static_query",
            "Static schema query",
            "Simple prompt",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "answer": {"type": "string"}
                }
            }),
        );

        assert!(!query.has_schema_template());

        // Schema generation should return the static schema
        let schema_vars: HashMap<String, serde_json::Value> = HashMap::new();
        let schema = query.generate_schema(&schema_vars).unwrap();

        // Should be the same as the original schema
        assert_eq!(schema, query.output_schema);
    }

    /// Test builder pattern with dynamic schema templates
    /// Validates that builder can create queries with both prompt and schema templates
    #[test]
    fn test0237_builder_with_schema_template() {
        let query = StructuredQueryBuilder::new()
            .name("builder_test")
            .description("Test builder with schema template")
            .prompt_template("Test prompt with {{ variable }}")
            .output_schema(serde_json::json!({"type": "object"}))
            .schema_template("{ \"type\": \"object\", \"properties\": { \"value\": { \"type\": \"integer\", \"minimum\": {{ min }} } } }")
            .metadata("test", "value")
            .build()
            .unwrap();

        assert_eq!(query.name, "builder_test");
        assert!(query.has_schema_template());
        assert_eq!(query.metadata.get("test"), Some(&"value".to_string()));

        // Test schema generation
        let mut schema_vars: HashMap<String, serde_json::Value> = HashMap::new();
        schema_vars.insert(
            "min".to_string(),
            serde_json::Value::Number(serde_json::Number::from(5)),
        );

        let schema = query.generate_schema(&schema_vars).unwrap();
        let properties = schema.as_object().unwrap().get("properties").unwrap();
        let value_prop = properties.as_object().unwrap().get("value").unwrap();
        assert_eq!(value_prop.get("minimum").unwrap().as_i64().unwrap(), 5);
    }
}
