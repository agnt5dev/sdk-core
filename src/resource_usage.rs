//! The worker container's CPU and memory use, as fractions of its limits,
//! for each capacity report.
//!
//! The runtime stops adding slots to a worker whose CPU or memory use has
//! reached the targets in its slot policy. All of a worker's runs share one
//! process, so a worker close to its memory limit that takes on more runs
//! can be killed with every run it holds. Usage is read from the
//! container's cgroup, v2 first and then v1. Off Linux, outside a cgroup, or
//! with no limit set it reads as 0, which the runtime treats as unknown.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
/// cgroup v1 reports an unset memory limit as a page-aligned `i64::MAX`.
const V1_UNLIMITED_BYTES: u64 = 1 << 60;

/// Samples the container's resource use between capacity reports. CPU use
/// is a rate, so each sample measures from the one before it.
pub(crate) struct ResourceSampler {
    root: PathBuf,
    previous_cpu: Option<CpuSample>,
}

#[derive(Clone, Copy)]
struct CpuSample {
    at: Instant,
    usage_usec: u64,
}

impl ResourceSampler {
    pub(crate) fn new() -> Self {
        Self::at(CGROUP_ROOT)
    }

    fn at(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            previous_cpu: None,
        }
    }

    /// Memory in use over the container's memory limit, in `[0, 1]`. Page
    /// cache the kernel can reclaim is left out, as Kubernetes does for its
    /// working set, so cached files don't make a worker look full.
    pub(crate) fn memory_usage(&self) -> f64 {
        let v2 = || {
            let limit = read_limit(&self.root.join("memory.max"))?;
            let current = read_u64(&self.root.join("memory.current"))?;
            let inactive = read_stat(&self.root.join("memory.stat"), "inactive_file").unwrap_or(0);
            Some((current.saturating_sub(inactive), limit))
        };
        let v1 = || {
            let dir = self.root.join("memory");
            let limit = read_limit(&dir.join("memory.limit_in_bytes"))?;
            let current = read_u64(&dir.join("memory.usage_in_bytes"))?;
            let inactive = read_stat(&dir.join("memory.stat"), "total_inactive_file").unwrap_or(0);
            Some((current.saturating_sub(inactive), limit))
        };
        v2().or_else(v1)
            .map_or(0.0, |(used, limit)| fraction(used as f64, limit as f64))
    }

    /// CPU time used since the previous sample over the CPU time the
    /// container may use in that interval, in `[0, 1]`. The first sample has
    /// nothing to measure from and reads as 0.
    pub(crate) fn cpu_usage(&mut self, now: Instant) -> f64 {
        let Some(usage_usec) = self.cpu_usage_usec() else {
            self.previous_cpu = None;
            return 0.0;
        };
        let Some(previous) = self.previous_cpu.replace(CpuSample {
            at: now,
            usage_usec,
        }) else {
            return 0.0;
        };
        let elapsed_usec = now.saturating_duration_since(previous.at).as_micros() as f64;
        let used_usec = usage_usec.saturating_sub(previous.usage_usec) as f64;
        fraction(used_usec, elapsed_usec * self.cpu_limit_cores())
    }

    fn cpu_usage_usec(&self) -> Option<u64> {
        read_stat(&self.root.join("cpu.stat"), "usage_usec").or_else(|| {
            read_u64(&self.root.join("cpuacct").join("cpuacct.usage")).map(|nanos| nanos / 1_000)
        })
    }

    /// The cores the container may use: its CPU quota, or every core the
    /// process can run on when it has none.
    fn cpu_limit_cores(&self) -> f64 {
        let v2 = || {
            let raw = fs::read_to_string(self.root.join("cpu.max")).ok()?;
            let mut fields = raw.split_whitespace();
            let quota = fields.next()?.parse::<f64>().ok()?;
            let period = fields.next()?.parse::<f64>().ok()?;
            Some(quota / period)
        };
        let v1 = || {
            let dir = self.root.join("cpu");
            let quota = read_trimmed(&dir.join("cpu.cfs_quota_us"))?
                .parse::<f64>()
                .ok()?;
            let period = read_trimmed(&dir.join("cpu.cfs_period_us"))?
                .parse::<f64>()
                .ok()?;
            Some(quota / period)
        };
        v2().or_else(v1)
            .filter(|cores| cores.is_finite() && *cores > 0.0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism().map_or(1.0, |cores| cores.get() as f64)
            })
    }
}

/// `used` over `limit`, clamped to `[0, 1]`, or 0 when there is no limit.
fn fraction(used: f64, limit: f64) -> f64 {
    if limit.is_finite() && limit > 0.0 {
        (used / limit).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn read_trimmed(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|raw| raw.trim().to_string())
}

fn read_u64(path: &Path) -> Option<u64> {
    read_trimmed(path)?.parse().ok()
}

/// A memory limit in bytes, or `None` when none is set: `max` on cgroup v2,
/// a value near `i64::MAX` on v1.
fn read_limit(path: &Path) -> Option<u64> {
    read_u64(path).filter(|limit| *limit > 0 && *limit < V1_UNLIMITED_BYTES)
}

/// One `key value` line of a cgroup stat file such as `memory.stat`.
fn read_stat(path: &Path, key: &str) -> Option<u64> {
    fs::read_to_string(path).ok()?.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        (name == key).then(|| value.trim().parse().ok()).flatten()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn write(root: &Path, file: &str, contents: &str) {
        let path = root.join(file);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn memory_is_the_working_set_over_the_limit() {
        let cgroup = tempfile::tempdir().unwrap();
        let root = cgroup.path();
        write(root, "memory.max", "1073741824\n");
        write(root, "memory.current", "805306368\n");
        write(
            root,
            "memory.stat",
            "anon 536870912\nfile 268435456\ninactive_file 268435456\n",
        );
        // 768 MiB charged, 256 MiB of it reclaimable cache, under 1 GiB.
        assert_eq!(ResourceSampler::at(root).memory_usage(), 0.5);

        write(root, "memory.current", "2147483648\n");
        write(root, "memory.stat", "inactive_file 0\n");
        assert_eq!(ResourceSampler::at(root).memory_usage(), 1.0);
    }

    #[test]
    fn memory_reads_cgroup_v1_and_is_unknown_without_a_limit() {
        let cgroup = tempfile::tempdir().unwrap();
        let root = cgroup.path();
        assert_eq!(ResourceSampler::at(root).memory_usage(), 0.0);

        write(root, "memory/memory.limit_in_bytes", "536870912\n");
        write(root, "memory/memory.usage_in_bytes", "268435456\n");
        write(
            root,
            "memory/memory.stat",
            "total_inactive_file 134217728\n",
        );
        assert_eq!(ResourceSampler::at(root).memory_usage(), 0.25);

        write(
            root,
            "memory/memory.limit_in_bytes",
            "9223372036854771712\n",
        );
        assert_eq!(ResourceSampler::at(root).memory_usage(), 0.0);

        write(root, "memory.max", "max\n");
        write(root, "memory.current", "268435456\n");
        assert_eq!(ResourceSampler::at(root).memory_usage(), 0.0);
    }

    #[test]
    fn cpu_is_the_time_used_over_the_quota_since_the_last_sample() {
        let cgroup = tempfile::tempdir().unwrap();
        let root = cgroup.path();
        write(root, "cpu.max", "200000 100000\n");
        write(root, "cpu.stat", "usage_usec 1000000\nuser_usec 900000\n");
        let mut sampler = ResourceSampler::at(root);
        let start = Instant::now();
        assert_eq!(sampler.cpu_usage(start), 0.0, "nothing to measure from yet");

        // One CPU-second in one second, on a two-core quota.
        write(root, "cpu.stat", "usage_usec 2000000\nuser_usec 1800000\n");
        assert_eq!(sampler.cpu_usage(start + Duration::from_secs(1)), 0.5);

        // Throttled at the quota: two CPU-seconds in the next second.
        write(root, "cpu.stat", "usage_usec 4000000\n");
        assert_eq!(sampler.cpu_usage(start + Duration::from_secs(2)), 1.0);
    }

    #[test]
    fn cpu_without_a_quota_is_measured_against_the_cores_available() {
        let cgroup = tempfile::tempdir().unwrap();
        let root = cgroup.path();
        write(root, "cpu.max", "max 100000\n");
        write(root, "cpu.stat", "usage_usec 0\n");
        let mut sampler = ResourceSampler::at(root);
        let start = Instant::now();
        sampler.cpu_usage(start);
        write(root, "cpu.stat", "usage_usec 1000000\n");
        let cores = std::thread::available_parallelism().unwrap().get() as f64;
        let usage = sampler.cpu_usage(start + Duration::from_secs(1));
        assert!((usage - (1.0 / cores).min(1.0)).abs() < 1e-9, "{usage}");
    }

    #[test]
    fn cpu_reads_cgroup_v1_and_is_unknown_without_a_cgroup() {
        let cgroup = tempfile::tempdir().unwrap();
        let root = cgroup.path();
        let mut sampler = ResourceSampler::at(root);
        let start = Instant::now();
        assert_eq!(sampler.cpu_usage(start), 0.0);
        assert_eq!(sampler.cpu_usage(start + Duration::from_secs(1)), 0.0);

        write(root, "cpu/cpu.cfs_quota_us", "100000\n");
        write(root, "cpu/cpu.cfs_period_us", "100000\n");
        write(root, "cpuacct/cpuacct.usage", "0\n");
        sampler.cpu_usage(start);
        write(root, "cpuacct/cpuacct.usage", "250000000\n");
        assert_eq!(sampler.cpu_usage(start + Duration::from_secs(1)), 0.25);
    }
}
