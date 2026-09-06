use super::{QueryDefinition, StructuredQuery};
use anyhow::Result;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tracing::{debug, error, warn};

/// Registry of all predefined structured queries
pub struct StructuredQueryRegistry {
    queries: HashMap<String, StructuredQuery>,
}

impl StructuredQueryRegistry {
    /// Create a new registry with default queries loaded from files
    pub fn new() -> Self {
        let mut registry = Self {
            queries: HashMap::new(),
        };

        // Load queries from external files
        if let Err(e) = registry.load_queries_from_files() {
            panic!("Failed to load structured queries from files: {}", e);
        }

        registry
    }

    pub fn query(&self, name: &str) -> Result<&StructuredQuery> {
        self.get(name).ok_or_else(|| {
            error!(
                "CRITICAL: '{}' structured query not found in registry",
                name
            );
            error!("Available queries: {:?}", self.list_queries());
            anyhow::anyhow!(
                "Document analysis structured query '{}' not found in registry",
                name
            )
        })
    }

    /// Load queries from embedded files using include_str! macros
    fn load_queries_from_files(&mut self) -> Result<()> {
        // Embed the queries.json file at compile time
        let queries_content = include_str!("queries.json");

        let query_definitions: Vec<QueryDefinition> = serde_json::from_str(queries_content)
            .map_err(|e| {
                anyhow::anyhow!("INVALID_QUERIES_JSON: Failed to parse embedded queries.json: {}", e)
            })?;

        for def in query_definitions {
            match self.load_query_from_embedded_definition(def) {
                Ok(query) => {
                    self.queries.insert(query.name.clone(), query);
                }
                Err(e) => {
                    warn!("Failed to load query: {}", e);
                }
            }
        }

        debug!(
            "Successfully loaded {} structured queries from embedded files",
            self.queries.len()
        );
        Ok(())
    }

    /// Load a single query from its definition using embedded files
    fn load_query_from_embedded_definition(&self, def: QueryDefinition) -> Result<StructuredQuery> {
        // Load embedded prompt template based on filename
        let prompt_template = match def.prompt_file.as_str() {
            "prompts/make_decision.tera" => include_str!("prompts/make_decision.tera"),
            "prompts/make_multiple_decisions.tera" => {
                include_str!("prompts/make_multiple_decisions.tera")
            }
            "prompts/dynamic_choice.tera" => include_str!("prompts/dynamic_choice.tera"),
            "prompts/semantic_same.tera" => include_str!("prompts/semantic_same.tera"),
            "prompts/semantic_classify.tera" => include_str!("prompts/semantic_classify.tera"),
            "prompts/semantic_score.tera" => include_str!("prompts/semantic_score.tera"),
            "prompts/semantic_verify.tera" => include_str!("prompts/semantic_verify.tera"),
            "prompts/semantic_route.tera" => include_str!("prompts/semantic_route.tera"),
            "prompts/semantic_normalize.tera" => include_str!("prompts/semantic_normalize.tera"),
            "prompts/semantic_summarize.tera" => include_str!("prompts/semantic_summarize.tera"),
            "prompts/semantic_ask.tera" => include_str!("prompts/semantic_ask.tera"),
            "prompts/semantic_explain.tera" => include_str!("prompts/semantic_explain.tera"),
            _ => {
                return Err(anyhow::anyhow!(
                    "PROMPT_FILE_NOT_FOUND: Unknown embedded prompt file: {}",
                    def.prompt_file
                ))
            }
        };

        // Load embedded schema content based on filename
        let schema_content = match def.schema_file.as_str() {
            "schemas/make_decision.json" => include_str!("schemas/make_decision.json"),
            "schemas/make_multiple_decisions.tera" => {
                include_str!("schemas/make_multiple_decisions.tera")
            }
            "schemas/dynamic_choice.tera" => include_str!("schemas/dynamic_choice.tera"),
            "schemas/semantic_same.json" => include_str!("schemas/semantic_same.json"),
            "schemas/semantic_classify.tera" => include_str!("schemas/semantic_classify.tera"),
            "schemas/semantic_score.tera" => include_str!("schemas/semantic_score.tera"),
            "schemas/semantic_verify.json" => include_str!("schemas/semantic_verify.json"),
            "schemas/semantic_route.tera" => include_str!("schemas/semantic_route.tera"),
            "schemas/semantic_normalize.json" => include_str!("schemas/semantic_normalize.json"),
            "schemas/semantic_summarize.json" => include_str!("schemas/semantic_summarize.json"),
            "schemas/semantic_ask.json" => include_str!("schemas/semantic_ask.json"),
            "schemas/semantic_explain.json" => include_str!("schemas/semantic_explain.json"),
            _ => {
                return Err(anyhow::anyhow!(
                    "SCHEMA_FILE_NOT_FOUND: Unknown embedded schema file: {}",
                    def.schema_file
                ))
            }
        };

        // Check if schema file is a Tera template (ends with .tera)
        let is_schema_template = def.schema_file.ends_with(".tera");

        let mut query = if is_schema_template {
            // Schema is a Tera template - create with template
            let dummy_schema = serde_json::json!({"type": "object"});
            StructuredQuery::new_with_schema_template(
                def.name,
                def.description,
                prompt_template.trim(),
                dummy_schema,
                schema_content.trim(),
            )
        } else {
            // Schema is static JSON - parse and use directly
            let output_schema: JsonValue = serde_json::from_str(schema_content).map_err(|e| {
                anyhow::anyhow!(
                    "INVALID_SCHEMA_JSON: Failed to parse embedded schema file {}: {}",
                    def.schema_file,
                    e
                )
            })?;

            StructuredQuery::new(
                def.name,
                def.description,
                prompt_template.trim(),
                output_schema,
            )
        };

        if let Some(metadata) = def.metadata {
            query.metadata = metadata;
        }

        Ok(query)
    }

    /// Register a new structured query
    pub fn register(&mut self, query: StructuredQuery) {
        self.queries.insert(query.name.clone(), query);
    }

    /// Get a query by name
    pub fn get(&self, name: &str) -> Option<&StructuredQuery> {
        self.queries.get(name)
    }

    /// List all available query names
    pub fn list_queries(&self) -> Vec<&String> {
        self.queries.keys().collect()
    }

    /// Get the number of loaded queries
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

impl Default for StructuredQueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}
