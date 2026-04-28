//! Worker-side coordinator routing based on deterministic ownership.
//!
//! Resolution priority:
//! 1. Fresh membership snapshot from control plane (if `AGNT5_CONTROL_PLANE_URL` is set)
//! 2. Cached snapshot from a previous fetch
//! 3. Static `AGNT5_COORDINATOR_MEMBERSHIP` env var
//! 4. Plain `AGNT5_COORDINATOR_ENDPOINT` (no owner routing)

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
    #[serde(default)]
    ready: Option<bool>,
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
        self.owner_for_key(worker_id)
    }

    fn owner_for_key(&self, key: &str) -> Option<&CoordinatorMember> {
        if self.members.is_empty() {
            return None;
        }
        let lookup = build_maglev_lookup(&self.members);
        let idx = (fnv1a64(key.as_bytes()) as usize) % lookup.len();
        self.members.get(lookup[idx])
    }

    /// Resolve the coordinator endpoint for a worker.
    ///
    /// Uses the same Maglev owner selection as the coordinator.
    pub fn endpoint_for_worker(&self, worker_id: &str, fallback: &str) -> String {
        match self.owner_for_worker(worker_id) {
            Some(owner) => normalize_address(&owner.address),
            None => fallback.to_string(),
        }
    }

    /// Return coordinator endpoints with the Maglev owner first.
    /// Useful for failover: if the primary owner is unreachable, try the next one.
    pub fn ranked_endpoints_for_worker(&self, worker_id: &str) -> Vec<String> {
        let Some(owner) = self.owner_for_worker(worker_id) else {
            return Vec::new();
        };
        let mut endpoints = Vec::with_capacity(self.members.len());
        endpoints.push(normalize_address(&owner.address));
        endpoints.extend(
            self.members
                .iter()
                .filter(|member| member.id != owner.id)
                .map(|member| normalize_address(&member.address)),
        );
        endpoints
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

        match client.get(&url).header("X-API-Key", &api_key).send().await {
            Ok(resp) if resp.status().is_success() => {
                let snapshot: MembershipSnapshot = resp.json().await.ok()?;
                let members: Vec<CoordinatorMember> = snapshot
                    .coordinators
                    .into_iter()
                    .filter(|c| c.status == "active" && c.ready.unwrap_or(true))
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

const MAGLEV_TABLE_SIZE: usize = 65_537;

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn build_maglev_lookup(members: &[CoordinatorMember]) -> Vec<usize> {
    if members.is_empty() {
        return Vec::new();
    }

    let m = MAGLEV_TABLE_SIZE;
    let mut next = vec![0usize; members.len()];
    let permutations: Vec<(usize, usize)> = members
        .iter()
        .map(|member| maglev_permutation(&member.id, m))
        .collect();
    let mut entry = vec![usize::MAX; m];
    let mut filled = 0usize;

    while filled < m {
        for (backend, (offset, skip)) in permutations.iter().enumerate() {
            let mut c = (*offset + next[backend] * *skip) % m;
            while entry[c] != usize::MAX {
                next[backend] += 1;
                c = (*offset + next[backend] * *skip) % m;
            }
            entry[c] = backend;
            next[backend] += 1;
            filled += 1;
            if filled == m {
                break;
            }
        }
    }

    entry
}

fn maglev_permutation(member_id: &str, table_size: usize) -> (usize, usize) {
    let offset = (fnv1a64(format!("{member_id}:offset").as_bytes()) as usize) % table_size;
    let skip = ((fnv1a64(format!("{member_id}:skip").as_bytes()) as usize) % (table_size - 1)) + 1;
    (offset, skip)
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
    fn endpoint_resolution_uses_maglev_owner() {
        let routing = CoordinatorRouting::from_membership(
            "node-a=runtime-1:34182,node-b=runtime-2:34182,node-c=http://runtime-3:34182",
        );
        let endpoint = routing.endpoint_for_worker("worker-abc", "http://lb:34182");
        assert!(endpoint.starts_with("http://"));
        assert_ne!(endpoint, "http://lb:34182");
    }

    #[test]
    fn maglev_owner_matches_coordinator_algorithm_for_known_worker() {
        let routing = CoordinatorRouting::from_membership(
            "node-a=runtime-1:34182,node-b=runtime-2:34182,node-c=runtime-3:34182",
        );
        assert_eq!(
            routing
                .owner_for_worker("worker-123")
                .map(|m| m.id.as_str()),
            Some("node-c")
        );
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
