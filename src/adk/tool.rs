//! Tool definitions and registration for the ADK.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{Result, SdkError};

/// Metadata for a registered tool.
#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Option<String>,
    pub output_schema: Option<String>,
}

/// Handle representing a tool registration.
#[derive(Clone)]
pub struct ToolHandle {
    name: String,
    registry: Arc<ToolRegistry>,
}

impl ToolHandle {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Retrieve the full definition for this tool.
    pub fn definition(&self) -> Result<ToolDefinition> {
        self.registry.get(&self.name)
    }
}

/// Registry of ADK tool definitions.
#[derive(Default)]
pub struct ToolRegistry {
    inner: Mutex<HashMap<String, ToolDefinition>>,
}

impl ToolRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(self: &Arc<Self>, definition: ToolDefinition) -> Result<ToolHandle> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| SdkError::Internal("tool registry mutex poisoned".to_string()))?;
        // Make registration idempotent - if tool already exists, just return success
        // This handles Python module reloads and multiple imports gracefully
        if !guard.contains_key(&definition.name) {
            guard.insert(definition.name.clone(), definition.clone());
        }
        Ok(ToolHandle {
            name: definition.name,
            registry: Arc::clone(self),
        })
    }

    pub fn get(self: &Arc<Self>, name: &str) -> Result<ToolDefinition> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| SdkError::Internal("tool registry mutex poisoned".to_string()))?;
        guard
            .get(name)
            .cloned()
            .ok_or_else(|| SdkError::Configuration {
                message: format!("Tool '{name}' not found"),
                field: Some("name".to_string()),
            })
    }

    pub fn list(self: &Arc<Self>) -> Result<Vec<ToolDefinition>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| SdkError::Internal("tool registry mutex poisoned".to_string()))?;
        Ok(guard.values().cloned().collect())
    }

    pub fn clear(self: &Arc<Self>) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| SdkError::Internal("tool registry mutex poisoned".to_string()))?;
        guard.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get_tool() {
        let registry = ToolRegistry::new();
        let definition = ToolDefinition {
            name: "search".to_string(),
            description: Some("Search the web".to_string()),
            input_schema: None,
            output_schema: None,
        };
        registry.register(definition).unwrap();

        let tools = registry.list().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "search");
    }
}
