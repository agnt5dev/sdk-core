// Graph database abstraction for knowledge relationships

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub mod memory_graph;

use crate::error::Result;

/// Node in a knowledge graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    /// Unique node identifier (e.g., "User:alice", "Topic:python")
    pub id: String,
    /// Node type (e.g., "User", "Topic", "Document")
    pub node_type: String,
    /// Optional properties
    pub properties: HashMap<String, serde_json::Value>,
}

/// Relationship between nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphRelationship {
    /// Unique relationship identifier
    pub id: String,
    /// Source node ID
    pub from_node: String,
    /// Target node ID
    pub to_node: String,
    /// Relationship type (e.g., "likes", "knows", "relates_to")
    pub relationship_type: String,
    /// Optional properties
    pub properties: HashMap<String, serde_json::Value>,
    /// Creation timestamp
    pub created_at: i64,
}

/// Result of graph traversal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphTraversalResult {
    /// Starting node
    pub start_node: String,
    /// All nodes found in traversal
    pub nodes: Vec<GraphNode>,
    /// All relationships found in traversal
    pub relationships: Vec<GraphRelationship>,
    /// Traversal depth reached
    pub depth: u32,
}

/// Query for finding relationships
#[derive(Debug, Clone)]
pub struct RelationshipQuery {
    /// Filter by source node
    pub from_node: Option<String>,
    /// Filter by target node
    pub to_node: Option<String>,
    /// Filter by relationship type
    pub relationship_type: Option<String>,
    /// Maximum results to return
    pub limit: usize,
}

impl Default for RelationshipQuery {
    fn default() -> Self {
        Self {
            from_node: None,
            to_node: None,
            relationship_type: None,
            limit: 100,
        }
    }
}

/// Filters for graph traversal
#[derive(Debug, Clone)]
pub struct TraversalFilters {
    /// Only follow these relationship types
    pub relationship_types: Option<Vec<String>>,
    /// Only traverse to these node types
    pub node_types: Option<Vec<String>>,
}

/// Core graph database trait for managing knowledge relationships
#[async_trait]
pub trait GraphDatabase: Send + Sync {
    /// Create or update a node in the graph
    ///
    /// # Arguments
    /// * `id` - Unique node identifier
    /// * `node_type` - Type of node (e.g., "User", "Topic")
    /// * `properties` - Optional node properties
    ///
    /// # Returns
    /// The node ID
    async fn upsert_node(
        &self,
        id: &str,
        node_type: &str,
        properties: HashMap<String, serde_json::Value>,
    ) -> Result<String>;

    /// Create a relationship between two nodes
    ///
    /// # Arguments
    /// * `from_node` - Source node ID
    /// * `to_node` - Target node ID
    /// * `relationship_type` - Type of relationship
    /// * `properties` - Optional relationship properties
    ///
    /// # Returns
    /// Unique relationship ID
    async fn create_relationship(
        &self,
        from_node: &str,
        to_node: &str,
        relationship_type: &str,
        properties: HashMap<String, serde_json::Value>,
    ) -> Result<String>;

    /// Query relationships matching criteria
    ///
    /// # Arguments
    /// * `query` - Query parameters
    ///
    /// # Returns
    /// Vector of matching relationships
    async fn query_relationships(&self, query: RelationshipQuery) -> Result<Vec<GraphRelationship>>;

    /// Traverse graph from a starting node
    ///
    /// # Arguments
    /// * `start_node` - Starting node ID
    /// * `max_depth` - Maximum depth to traverse
    /// * `filters` - Optional filters for traversal
    ///
    /// # Returns
    /// Graph traversal result with all nodes and relationships
    async fn traverse(
        &self,
        start_node: &str,
        max_depth: u32,
        filters: Option<TraversalFilters>,
    ) -> Result<GraphTraversalResult>;

    /// Get a node by its ID
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    ///
    /// # Returns
    /// Node if found, None otherwise
    async fn get_node(&self, node_id: &str) -> Result<Option<GraphNode>>;

    /// Get a relationship by its ID
    ///
    /// # Arguments
    /// * `relationship_id` - Relationship identifier
    ///
    /// # Returns
    /// Relationship if found, None otherwise
    async fn get_relationship(&self, relationship_id: &str) -> Result<Option<GraphRelationship>>;

    /// Delete a relationship by its ID
    ///
    /// # Arguments
    /// * `relationship_id` - Relationship identifier
    ///
    /// # Returns
    /// True if deleted, false if not found
    async fn delete_relationship(&self, relationship_id: &str) -> Result<bool>;

    /// Delete a node and all its relationships
    ///
    /// # Arguments
    /// * `node_id` - Node identifier
    ///
    /// # Returns
    /// True if deleted, false if not found
    async fn delete_node(&self, node_id: &str) -> Result<bool>;
}

impl fmt::Display for GraphNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.node_type, self.id)
    }
}

impl fmt::Display for GraphRelationship {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} -[{}]-> {}",
            self.from_node, self.relationship_type, self.to_node
        )
    }
}
