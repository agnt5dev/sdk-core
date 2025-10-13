// Core types for vector database operations
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// A vector entry containing the vector data and associated metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorEntry {
    /// Unique identifier for this vector
    pub id: String,

    /// The vector data (embeddings)
    pub vector: Vec<f32>,

    /// Associated metadata (documents, tags, etc.)
    pub metadata: VectorMetadata,
}

/// Metadata associated with a vector entry
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VectorMetadata {
    /// Original text content (optional)
    pub text: Option<String>,

    /// Source document information
    pub source: Option<String>,

    /// Document chunk index
    pub chunk_index: Option<u32>,

    /// Additional arbitrary metadata
    pub extra: HashMap<String, Value>,
}

impl VectorMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_text(mut self, text: String) -> Self {
        self.text = Some(text);
        self
    }

    pub fn with_source(mut self, source: String) -> Self {
        self.source = Some(source);
        self
    }

    pub fn with_chunk_index(mut self, index: u32) -> Self {
        self.chunk_index = Some(index);
        self
    }

    pub fn with_extra<T: Serialize>(mut self, key: String, value: T) -> Self {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.extra.insert(key, json_value);
        }
        self
    }
}

/// Query for searching vectors
#[derive(Debug, Clone)]
pub struct SearchQuery {
    /// Query vector to search for similar vectors
    pub vector: Vec<f32>,

    /// Maximum number of results to return
    pub limit: u32,

    /// Minimum similarity score (0.0 to 1.0)
    pub min_score: Option<f32>,

    /// Metadata filter
    pub filter: Option<VectorFilter>,

    /// Distance metric to use for search
    pub distance_metric: Option<DistanceMetric>,

    /// Include vector data in results
    pub include_vectors: bool,

    /// Include metadata in results
    pub include_metadata: bool,
}

impl SearchQuery {
    pub fn new(vector: Vec<f32>) -> Self {
        Self {
            vector,
            limit: 10,
            min_score: None,
            filter: None,
            distance_metric: None,
            include_vectors: false,
            include_metadata: true,
        }
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_min_score(mut self, score: f32) -> Self {
        self.min_score = Some(score);
        self
    }

    pub fn with_filter(mut self, filter: VectorFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn include_vectors(mut self) -> Self {
        self.include_vectors = true;
        self
    }

    pub fn include_metadata(mut self) -> Self {
        self.include_metadata = true;
        self
    }
}

/// Result from a vector search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Vector entry ID
    pub id: String,

    /// Similarity score (0.0 to 1.0, higher is more similar)
    pub score: f32,

    /// Distance value (metric-dependent)
    pub distance: f32,

    /// Vector data (if requested)
    pub vector: Option<Vec<f32>>,

    /// Metadata (if requested)
    pub metadata: Option<VectorMetadata>,
}

/// Filter for querying vectors by metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorFilter {
    /// Must match all conditions (AND)
    pub must: Vec<FilterCondition>,

    /// Must match at least one condition (OR)
    pub should: Vec<FilterCondition>,

    /// Must not match any condition (NOT)
    pub must_not: Vec<FilterCondition>,
}

impl VectorFilter {
    pub fn new() -> Self {
        Self {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
        }
    }

    pub fn must(mut self, condition: FilterCondition) -> Self {
        self.must.push(condition);
        self
    }

    pub fn should(mut self, condition: FilterCondition) -> Self {
        self.should.push(condition);
        self
    }

    pub fn must_not(mut self, condition: FilterCondition) -> Self {
        self.must_not.push(condition);
        self
    }
}

impl Default for VectorFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Individual filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterCondition {
    /// Field name to filter on
    pub field: String,

    /// Filter operation
    pub operation: FilterOperation,

    /// Value to compare against
    pub value: Value,
}

impl FilterCondition {
    pub fn equals(field: String, value: Value) -> Self {
        Self {
            field,
            operation: FilterOperation::Equals,
            value,
        }
    }

    pub fn not_equals(field: String, value: Value) -> Self {
        Self {
            field,
            operation: FilterOperation::NotEquals,
            value,
        }
    }

    pub fn contains(field: String, value: Value) -> Self {
        Self {
            field,
            operation: FilterOperation::Contains,
            value,
        }
    }

    pub fn in_list(field: String, values: Vec<Value>) -> Self {
        Self {
            field,
            operation: FilterOperation::In,
            value: Value::Array(values),
        }
    }
}

/// Filter operations for metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOperation {
    /// Exact match
    Equals,

    /// Not equal
    NotEquals,

    /// Contains substring (for strings)
    Contains,

    /// Value in list
    In,

    /// Value not in list
    NotIn,

    /// Greater than (for numbers)
    GreaterThan,

    /// Less than (for numbers)
    LessThan,

    /// Greater than or equal (for numbers)
    GreaterThanOrEqual,

    /// Less than or equal (for numbers)
    LessThanOrEqual,
}

/// Distance metrics for vector similarity
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Cosine similarity (most common for text embeddings)
    Cosine,

    /// Euclidean distance
    Euclidean,

    /// Dot product
    DotProduct,

    /// Manhattan distance
    Manhattan,
}

impl Default for DistanceMetric {
    fn default() -> Self {
        DistanceMetric::Cosine
    }
}

impl std::fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistanceMetric::Cosine => write!(f, "cosine"),
            DistanceMetric::Euclidean => write!(f, "euclidean"),
            DistanceMetric::DotProduct => write!(f, "dot_product"),
            DistanceMetric::Manhattan => write!(f, "manhattan"),
        }
    }
}

/// Configuration for a vector collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    /// Collection name
    pub name: String,

    /// Vector dimension
    pub dimension: u32,

    /// Distance metric to use
    pub distance_metric: DistanceMetric,

    /// Description
    pub description: Option<String>,

    /// Additional configuration
    pub config: HashMap<String, Value>,
}

impl Collection {
    pub fn new(name: String, dimension: u32) -> Self {
        Self {
            name,
            dimension,
            distance_metric: DistanceMetric::default(),
            description: None,
            config: HashMap::new(),
        }
    }

    pub fn with_distance_metric(mut self, metric: DistanceMetric) -> Self {
        self.distance_metric = metric;
        self
    }

    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    pub fn with_config<T: Serialize>(mut self, key: String, value: T) -> Self {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.config.insert(key, json_value);
        }
        self
    }
}

/// Batch operation for efficient bulk operations
#[derive(Debug, Clone)]
pub struct BatchOperation {
    /// Collection name
    pub collection_name: String,

    /// Operations to perform
    pub operations: Vec<VectorOperation>,
}

/// Individual vector operation
#[derive(Debug, Clone)]
pub enum VectorOperation {
    /// Insert or update a vector
    Upsert(VectorEntry),

    /// Delete a vector by ID
    Delete(String),
}

impl BatchOperation {
    pub fn new(collection_name: String) -> Self {
        Self {
            collection_name,
            operations: Vec::new(),
        }
    }

    pub fn upsert(mut self, entry: VectorEntry) -> Self {
        self.operations.push(VectorOperation::Upsert(entry));
        self
    }

    pub fn delete(mut self, id: String) -> Self {
        self.operations.push(VectorOperation::Delete(id));
        self
    }
}
