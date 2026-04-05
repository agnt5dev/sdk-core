//! Worker-side coordinator routing based on deterministic ownership.
//!
//! Resolution priority:
//! 1. Fresh membership snapshot from control plane (if `AGNT5_CONTROL_PLANE_URL` is set)
//! 2. Cached snapshot from a previous fetch
//! 3. Static `AGNT5_COORDINATOR_MEMBERSHIP` env var
//! 4. Plain `AGNT5_COORDINATOR_ENDPOINT` (no rendezvous routing)

use serde::Deserialize;
use std::sync::RwLock;
use tracing::{debug, warn};

/// Cached membership snapshot, shared across reconnect cycles.
static CACHED_ROUTING: RwLock<Option<CoordinatorRouting>> = RwLock::new(None);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorMember {
    pub id: String,
    pub address: String,
}

#[derive(Debug, Clone, Default)]
pub struct CoordinatorRouting {
    members: Vec<CoordinatorMember>,
}

/// Response from GET /api/v1/internal/dataplane/coordinators/membership
#[derive(Deserialize)]
struct MembershipSnapshot {
    #[allow(dead_code)]
    membership_epoch: i64,
    coordinators: Vec<SnapshotCoordinator>,
}

#[derive(Deserialize)]
struct SnapshotCoordinator {
    coordinator_id: String,
    grpc_endpoint: String,
    status: String,
}

impl CoordinatorRouting {
    /// Build routing from env (static membership only).
    pub fn from_env() -> Self {
        let raw = std::env::var("AGNT5_COORDINATOR_MEMBERSHIP")
            .ok()
            .filter(|v| !v.trim().is_empty());

        match raw {
            Some(raw) => Self::from_membership(&raw),
            None => Self::default(),
        }
    }

    /// Build routing from a comma-separated `id=host:port` string.
    pub fn from_membership(raw: &str) -> Self {
        let mut members = Vec::new();
        for entry in raw.split(',').filter(|s| !s.trim().is_empty()) {
            let Some((id, address)) = entry.split_once('=') else {
                continue;
            };
            let id = id.trim();
            let address = address.trim();
            if id.is_empty() || address.is_empty() {
                continue;
            }
            members.push(CoordinatorMember {
                id: id.to_string(),
                address: address.to_string(),
            });
        }

        members.sort_by(|a, b| a.id.cmp(&b.id));
        members.dedup_by(|a, b| a.id == b.id);

        Self { members }
    }

    pub fn owner_for_worker(&self, worker_id: &str) -> Option<&CoordinatorMember> {
        self.members
            .iter()
            .max_by_key(|member| rendezvous_score(worker_id, &member.id))
    }

    /// Resolve the coordinator endpoint for a worker.
    ///
    /// Uses the ranking from rendezvous hashing. If the primary owner is
    /// unreachable, callers can use `ranked_endpoints_for_worker` to try
    /// the next coordinator in the ranking.
    pub fn endpoint_for_worker(&self, worker_id: &str, fallback: &str) -> String {
        match self.owner_for_worker(worker_id) {
            Some(owner) => normalize_address(&owner.address),
            None => fallback.to_string(),
        }
    }

    /// Return all coordinator endpoints ranked by rendezvous score (best first).
    /// Useful for failover: if the primary owner is unreachable, try the next one.
    pub fn ranked_endpoints_for_worker(&self, worker_id: &str) -> Vec<String> {
        let mut scored: Vec<_> = self
            .members
            .iter()
            .map(|m| (rendezvous_score(worker_id, &m.id), &m.address))
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0)); // descending score
        scored
            .into_iter()
            .map(|(_, addr)| normalize_address(addr))
            .collect()
    }

    /// Resolve routing with the full priority chain:
    /// 1. Fetch fresh snapshot from control plane (if configured)
    /// 2. Use cached snapshot
    /// 3. Fall back to static env membership
    pub async fn resolve(worker_id: &str, fallback_endpoint: &str) -> String {
        // Try fetching from control plane
        if let Some(routing) = Self::fetch_from_control_plane().await {
            let endpoint = routing.endpoint_for_worker(worker_id, fallback_endpoint);
            // Cache for future use
            if let Ok(mut cache) = CACHED_ROUTING.write() {
                *cache = Some(routing);
            }
            return endpoint;
        }

        // Try cached snapshot
        if let Ok(cache) = CACHED_ROUTING.read() {
            if let Some(routing) = cache.as_ref() {
                let endpoint = routing.endpoint_for_worker(worker_id, fallback_endpoint);
                debug!("Using cached membership snapshot for routing");
                return endpoint;
            }
        }

        // Fall back to static env
        let routing = Self::from_env();
        routing.endpoint_for_worker(worker_id, fallback_endpoint)
    }

    /// Fetch membership from the control plane. Returns None if not configured or on error.
    async fn fetch_from_control_plane() -> Option<Self> {
        let base_url = std::env::var("AGNT5_CONTROL_PLANE_URL").ok()?;
        if base_url.is_empty() {
            return None;
        }
        let api_key = std::env::var("AGNT5_CONTROL_PLANE_KEY").unwrap_or_default();

        let url = format!("{base_url}/api/v1/internal/dataplane/coordinators/membership");

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .ok()?;

        match client
            .get(&url)
            .header("X-API-Key", &api_key)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                let snapshot: MembershipSnapshot = resp.json().await.ok()?;
                let members: Vec<CoordinatorMember> = snapshot
                    .coordinators
                    .into_iter()
                    .filter(|c| c.status == "active")
                    .map(|c| CoordinatorMember {
                        id: c.coordinator_id,
                        address: c.grpc_endpoint,
                    })
                    .collect();

                if members.is_empty() {
                    warn!("Membership snapshot returned no active coordinators");
                    return None;
                }

                debug!(
                    coordinators = members.len(),
                    "Fetched membership snapshot from control plane"
                );

                let mut routing = Self { members };
                routing.members.sort_by(|a, b| a.id.cmp(&b.id));
                routing.members.dedup_by(|a, b| a.id == b.id);
                Some(routing)
            }
            Ok(resp) => {
                warn!(
                    status = %resp.status(),
                    "Failed to fetch membership snapshot"
                );
                None
            }
            Err(e) => {
                debug!(error = %e, "Control plane unreachable for membership snapshot");
                None
            }
        }
    }
}

fn rendezvous_score(worker_id: &str, member_id: &str) -> u64 {
    fn fnv1a64(bytes: &[u8]) -> u64 {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    let mut key = Vec::with_capacity(worker_id.len() + member_id.len() + 1);
    key.extend_from_slice(worker_id.as_bytes());
    key.push(b':');
    key.extend_from_slice(member_id.as_bytes());
    fnv1a64(&key)
}

fn normalize_address(address: &str) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

#[cfg(test)]
mod tests {
    use super::CoordinatorRouting;

    #[test]
    fn endpoint_resolution_uses_rendezvous_owner() {
        let routing = CoordinatorRouting::from_membership(
            "node-a=runtime-1:34182,node-b=runtime-2:34182,node-c=http://runtime-3:34182",
        );
        let endpoint = routing.endpoint_for_worker("worker-abc", "http://lb:34182");
        assert!(endpoint.starts_with("http://"));
        assert_ne!(endpoint, "http://lb:34182");
    }

    #[test]
    fn empty_membership_falls_back_to_configured_endpoint() {
        let routing = CoordinatorRouting::default();
        assert_eq!(
            routing.endpoint_for_worker("worker-abc", "http://lb:34182"),
            "http://lb:34182"
        );
    }
}
