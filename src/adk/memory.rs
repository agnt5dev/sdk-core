//! Memory scaffolding for the ADK.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{Result, SdkError};

/// Memory entry stored in the in-memory backend.
#[derive(Debug, Clone)]
pub struct MemoryItem {
    pub key: String,
    pub content: String,
    pub metadata: HashMap<String, String>,
}

impl MemoryItem {
    pub fn new(key: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            content: content.into(),
            metadata: HashMap::new(),
        }
    }
}

/// Memory handle exposed to higher layers.
#[derive(Clone)]
pub struct MemoryHandle {
    backend: Arc<dyn MemoryBackend>,
}

impl MemoryHandle {
    pub fn new(backend: Arc<dyn MemoryBackend>) -> Self {
        Self { backend }
    }

    pub fn new_placeholder() -> Self {
        Self::new(Arc::new(InMemoryMemoryBackend::default()))
    }

    pub fn store(
        &self,
        key: impl Into<String>,
        content: impl Into<String>,
        metadata: HashMap<String, String>,
    ) -> Result<String> {
        self.backend.store(key.into(), content.into(), metadata)
    }

    pub fn search(&self, query: &str, limit: Option<usize>) -> Result<Vec<MemoryItem>> {
        self.backend.search(query, limit)
    }

    pub fn recall(&self, keys: &[String]) -> Result<Vec<MemoryItem>> {
        self.backend.recall(keys)
    }

    pub fn forget(&self, keys: &[String]) -> Result<usize> {
        self.backend.forget(keys)
    }
}

/// Trait that durable memory backends will implement.
pub trait MemoryBackend: Send + Sync {
    fn store(
        &self,
        key: String,
        content: String,
        metadata: HashMap<String, String>,
    ) -> Result<String>;
    fn search(&self, query: &str, limit: Option<usize>) -> Result<Vec<MemoryItem>>;
    fn recall(&self, keys: &[String]) -> Result<Vec<MemoryItem>>;
    fn forget(&self, keys: &[String]) -> Result<usize>;
}

/// Simple in-memory implementation for development/testing.
#[derive(Default)]
pub struct InMemoryMemoryBackend {
    inner: Mutex<HashMap<String, MemoryItem>>,
}

impl InMemoryMemoryBackend {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HashMap<String, MemoryItem>>> {
        self.inner
            .lock()
            .map_err(|_| SdkError::Internal("memory backend mutex poisoned".to_string()))
    }
}

impl MemoryBackend for InMemoryMemoryBackend {
    fn store(
        &self,
        key: String,
        content: String,
        metadata: HashMap<String, String>,
    ) -> Result<String> {
        let mut guard = self.lock()?;
        guard.insert(
            key.clone(),
            MemoryItem {
                key: key.clone(),
                content,
                metadata,
            },
        );
        Ok(key)
    }

    fn search(&self, query: &str, limit: Option<usize>) -> Result<Vec<MemoryItem>> {
        let guard = self.lock()?;
        let mut hits: Vec<_> = guard
            .values()
            .filter(|item| item.content.to_lowercase().contains(&query.to_lowercase()))
            .cloned()
            .collect();
        if let Some(limit) = limit {
            hits.truncate(limit);
        }
        Ok(hits)
    }

    fn recall(&self, keys: &[String]) -> Result<Vec<MemoryItem>> {
        let guard = self.lock()?;
        Ok(keys
            .iter()
            .filter_map(|key| guard.get(key).cloned())
            .collect())
    }

    fn forget(&self, keys: &[String]) -> Result<usize> {
        let mut guard = self.lock()?;
        let mut removed = 0;
        for key in keys {
            if guard.remove(key).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_store_search_recall_forget() {
        let memory = MemoryHandle::new_placeholder();
        let mut metadata = HashMap::new();
        metadata.insert("tag".to_string(), "customer".to_string());

        memory
            .store("user:1", "User likes detailed reports", metadata)
            .unwrap();

        let hits = memory.search("detailed", Some(10)).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "user:1");

        let keys = vec!["user:1".to_string()];
        let recall = memory.recall(&keys).unwrap();
        assert_eq!(recall.len(), 1);

        let removed = memory.forget(&keys).unwrap();
        assert_eq!(removed, 1);
        assert!(memory.search("detailed", None).unwrap().is_empty());
    }
}
