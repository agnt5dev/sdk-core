// In-memory graph database implementation for testing and simple use cases

use super::{
    GraphDatabase, GraphNode, GraphRelationship, GraphTraversalResult, RelationshipQuery,
    TraversalFilters,
};
use crate::error::{Result, SdkError};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// In-memory graph database
///
/// Provides a simple graph database implementation using in-memory hash maps.
/// Suitable for testing and small-scale use cases.
///
/// Features:
/// - Thread-safe using RwLock
/// - Bidirectional relationship indexing for fast queries
/// - BFS traversal with cycle detection
/// - Scoped isolation (multiple instances per scope)
pub struct MemoryGraphDatabase {
    /// Nodes indexed by ID
    nodes: Arc<RwLock<HashMap<String, GraphNode>>>,
    /// Relationships indexed by ID
    relationships: Arc<RwLock<HashMap<String, GraphRelationship>>>,
    /// Index: from_node -> relationship IDs
    from_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Index: to_node -> relationship IDs
    to_index: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Scope identifier for isolation
    scope: String,
}

impl MemoryGraphDatabase {
    /// Create a new in-memory graph database
    ///
    /// # Arguments
    /// * `scope` - Scope identifier for isolation (e.g., "user:123", "session:abc")
    pub fn new(scope: String) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            relationships: Arc::new(RwLock::new(HashMap::new())),
            from_index: Arc::new(RwLock::new(HashMap::new())),
            to_index: Arc::new(RwLock::new(HashMap::new())),
            scope,
        }
    }

    /// Helper to add relationship to indices
    async fn index_relationship(&self, rel_id: &str, from_node: &str, to_node: &str) {
        let mut from_idx = self.from_index.write().await;
        from_idx
            .entry(from_node.to_string())
            .or_insert_with(Vec::new)
            .push(rel_id.to_string());

        let mut to_idx = self.to_index.write().await;
        to_idx
            .entry(to_node.to_string())
            .or_insert_with(Vec::new)
            .push(rel_id.to_string());
    }

    /// Helper to remove relationship from indices
    async fn unindex_relationship(&self, rel_id: &str, from_node: &str, to_node: &str) {
        let mut from_idx = self.from_index.write().await;
        if let Some(rels) = from_idx.get_mut(from_node) {
            rels.retain(|id| id != rel_id);
        }

        let mut to_idx = self.to_index.write().await;
        if let Some(rels) = to_idx.get_mut(to_node) {
            rels.retain(|id| id != rel_id);
        }
    }
}

#[async_trait]
impl GraphDatabase for MemoryGraphDatabase {
    async fn upsert_node(
        &self,
        id: &str,
        node_type: &str,
        properties: HashMap<String, Value>,
    ) -> Result<String> {
        let node = GraphNode {
            id: id.to_string(),
            node_type: node_type.to_string(),
            properties,
        };

        let mut nodes = self.nodes.write().await;
        nodes.insert(id.to_string(), node);

        Ok(id.to_string())
    }

    async fn create_relationship(
        &self,
        from_node: &str,
        to_node: &str,
        relationship_type: &str,
        properties: HashMap<String, Value>,
    ) -> Result<String> {
        // Check that both nodes exist
        let nodes = self.nodes.read().await;
        if !nodes.contains_key(from_node) {
            return Err(SdkError::InvalidInput(format!(
                "Source node '{}' not found",
                from_node
            )));
        }
        if !nodes.contains_key(to_node) {
            return Err(SdkError::InvalidInput(format!(
                "Target node '{}' not found",
                to_node
            )));
        }
        drop(nodes); // Release read lock

        // Create relationship
        let rel_id = Uuid::new_v4().to_string();
        let relationship = GraphRelationship {
            id: rel_id.clone(),
            from_node: from_node.to_string(),
            to_node: to_node.to_string(),
            relationship_type: relationship_type.to_string(),
            properties,
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        // Store relationship
        let mut relationships = self.relationships.write().await;
        relationships.insert(rel_id.clone(), relationship);
        drop(relationships); // Release write lock

        // Update indices
        self.index_relationship(&rel_id, from_node, to_node).await;

        Ok(rel_id)
    }

    async fn query_relationships(&self, query: RelationshipQuery) -> Result<Vec<GraphRelationship>> {
        let relationships = self.relationships.read().await;
        let from_idx = self.from_index.read().await;
        let to_idx = self.to_index.read().await;

        // Find candidate relationship IDs based on indices
        let mut candidate_ids: Option<HashSet<String>> = None;

        if let Some(from_node) = &query.from_node {
            if let Some(ids) = from_idx.get(from_node) {
                let id_set: HashSet<String> = ids.iter().cloned().collect();
                candidate_ids = Some(match candidate_ids {
                    None => id_set,
                    Some(existing) => existing.intersection(&id_set).cloned().collect(),
                });
            } else {
                // No relationships from this node
                return Ok(Vec::new());
            }
        }

        if let Some(to_node) = &query.to_node {
            if let Some(ids) = to_idx.get(to_node) {
                let id_set: HashSet<String> = ids.iter().cloned().collect();
                candidate_ids = Some(match candidate_ids {
                    None => id_set,
                    Some(existing) => existing.intersection(&id_set).cloned().collect(),
                });
            } else {
                // No relationships to this node
                return Ok(Vec::new());
            }
        }

        // If no index filters, consider all relationships
        let candidates: Vec<GraphRelationship> = match candidate_ids {
            Some(ids) => ids
                .iter()
                .filter_map(|id| relationships.get(id).cloned())
                .collect(),
            None => relationships.values().cloned().collect(),
        };

        // Apply type filter
        let mut results: Vec<GraphRelationship> = if let Some(rel_type) = &query.relationship_type {
            candidates
                .into_iter()
                .filter(|r| &r.relationship_type == rel_type)
                .collect()
        } else {
            candidates
        };

        // Sort by creation time (newest first) and apply limit
        results.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        results.truncate(query.limit);

        Ok(results)
    }

    async fn traverse(
        &self,
        start_node: &str,
        max_depth: u32,
        filters: Option<TraversalFilters>,
    ) -> Result<GraphTraversalResult> {
        let nodes_map = self.nodes.read().await;
        let relationships_map = self.relationships.read().await;
        let from_idx = self.from_index.read().await;

        // Check start node exists
        if !nodes_map.contains_key(start_node) {
            return Err(SdkError::InvalidInput(format!(
                "Start node '{}' not found",
                start_node
            )));
        }

        // BFS traversal with depth tracking
        let mut queue: VecDeque<(String, u32)> = VecDeque::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut result_nodes: Vec<GraphNode> = Vec::new();
        let mut result_relationships: Vec<GraphRelationship> = Vec::new();

        // Start traversal
        queue.push_back((start_node.to_string(), 0));
        visited.insert(start_node.to_string());

        while let Some((current_node, depth)) = queue.pop_front() {
            // Add current node to results
            if let Some(node) = nodes_map.get(&current_node) {
                result_nodes.push(node.clone());
            }

            // Stop if max depth reached
            if depth >= max_depth {
                continue;
            }

            // Get outgoing relationships
            if let Some(rel_ids) = from_idx.get(&current_node) {
                for rel_id in rel_ids {
                    if let Some(rel) = relationships_map.get(rel_id) {
                        // Apply relationship type filter
                        if let Some(ref filters) = filters {
                            if let Some(ref allowed_types) = filters.relationship_types {
                                if !allowed_types.contains(&rel.relationship_type) {
                                    continue;
                                }
                            }
                        }

                        // Add relationship to results
                        result_relationships.push(rel.clone());

                        // Queue target node if not visited
                        if !visited.contains(&rel.to_node) {
                            // Apply node type filter
                            if let Some(target_node) = nodes_map.get(&rel.to_node) {
                                if let Some(ref filters) = filters {
                                    if let Some(ref allowed_types) = filters.node_types {
                                        if !allowed_types.contains(&target_node.node_type) {
                                            continue;
                                        }
                                    }
                                }

                                visited.insert(rel.to_node.clone());
                                queue.push_back((rel.to_node.clone(), depth + 1));
                            }
                        }
                    }
                }
            }
        }

        Ok(GraphTraversalResult {
            start_node: start_node.to_string(),
            nodes: result_nodes,
            relationships: result_relationships,
            depth: max_depth,
        })
    }

    async fn get_node(&self, node_id: &str) -> Result<Option<GraphNode>> {
        let nodes = self.nodes.read().await;
        Ok(nodes.get(node_id).cloned())
    }

    async fn get_relationship(&self, relationship_id: &str) -> Result<Option<GraphRelationship>> {
        let relationships = self.relationships.read().await;
        Ok(relationships.get(relationship_id).cloned())
    }

    async fn delete_relationship(&self, relationship_id: &str) -> Result<bool> {
        let mut relationships = self.relationships.write().await;

        if let Some(rel) = relationships.remove(relationship_id) {
            drop(relationships); // Release write lock

            // Remove from indices
            self.unindex_relationship(relationship_id, &rel.from_node, &rel.to_node)
                .await;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn delete_node(&self, node_id: &str) -> Result<bool> {
        // Remove node
        let mut nodes = self.nodes.write().await;
        let existed = nodes.remove(node_id).is_some();
        drop(nodes); // Release write lock

        if !existed {
            return Ok(false);
        }

        // Find and remove all relationships involving this node
        let from_idx = self.from_index.read().await;
        let to_idx = self.to_index.read().await;

        let mut rel_ids_to_delete: HashSet<String> = HashSet::new();

        if let Some(ids) = from_idx.get(node_id) {
            rel_ids_to_delete.extend(ids.clone());
        }
        if let Some(ids) = to_idx.get(node_id) {
            rel_ids_to_delete.extend(ids.clone());
        }

        drop(from_idx);
        drop(to_idx);

        // Delete each relationship
        for rel_id in rel_ids_to_delete {
            self.delete_relationship(&rel_id).await?;
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_graph_basic_operations() {
        let graph = MemoryGraphDatabase::new("test-scope".to_string());

        // Create nodes
        let alice = graph
            .upsert_node("User:alice", "User", HashMap::new())
            .await
            .unwrap();
        let python = graph
            .upsert_node("Language:python", "Language", HashMap::new())
            .await
            .unwrap();

        assert_eq!(alice, "User:alice");
        assert_eq!(python, "Language:python");

        // Create relationship
        let rel_id = graph
            .create_relationship("User:alice", "Language:python", "likes", HashMap::new())
            .await
            .unwrap();

        assert!(!rel_id.is_empty());

        // Query relationships
        let query = RelationshipQuery {
            from_node: Some("User:alice".to_string()),
            ..Default::default()
        };
        let rels = graph.query_relationships(query).await.unwrap();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].relationship_type, "likes");

        // Traverse graph
        let result = graph.traverse("User:alice", 1, None).await.unwrap();
        assert_eq!(result.nodes.len(), 2); // Alice + Python
        assert_eq!(result.relationships.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_graph_traversal_with_filters() {
        let graph = MemoryGraphDatabase::new("test-scope".to_string());

        // Create a small knowledge graph
        graph
            .upsert_node("User:alice", "User", HashMap::new())
            .await
            .unwrap();
        graph
            .upsert_node("Topic:ai", "Topic", HashMap::new())
            .await
            .unwrap();
        graph
            .upsert_node("Topic:ml", "Topic", HashMap::new())
            .await
            .unwrap();
        graph
            .upsert_node("Document:paper1", "Document", HashMap::new())
            .await
            .unwrap();

        // Create relationships
        graph
            .create_relationship("User:alice", "Topic:ai", "interested_in", HashMap::new())
            .await
            .unwrap();
        graph
            .create_relationship("Topic:ai", "Topic:ml", "subtopic", HashMap::new())
            .await
            .unwrap();
        graph
            .create_relationship("Topic:ml", "Document:paper1", "references", HashMap::new())
            .await
            .unwrap();

        // Traverse with depth 2, only "interested_in" and "subtopic" relationships
        let filters = TraversalFilters {
            relationship_types: Some(vec!["interested_in".to_string(), "subtopic".to_string()]),
            node_types: None,
        };

        let result = graph
            .traverse("User:alice", 2, Some(filters))
            .await
            .unwrap();

        // Should find Alice, AI topic, and ML topic
        assert_eq!(result.nodes.len(), 3);
        // Should find 2 relationships (interested_in, subtopic)
        assert_eq!(result.relationships.len(), 2);
    }
}
