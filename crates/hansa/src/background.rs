//! Background saga refresh.
//!
//! v0.1 forces the application to call [`Hansa::refresh_saga`] after
//! every meaningful batch of inserts. That works for tests and demos but
//! is a footgun in long-running agents: forgetting to refresh leaves
//! peers querying a stale digest of your memory.
//!
//! v0.1.x adds an opt-in background task that polls the local tenant's
//! record count and triggers a rebuild whenever it has grown by more
//! than `threshold_ratio` since the last build. The task runs on a
//! standard `std::thread`; no Tokio dependency.
//!
//! ```rust,ignore
//! let handle = hansa.start_background_refresh(
//!     BackgroundRefreshConfig::default(),
//!     || vec![],   // tag provider (rigging v0.1 has no tag iterator)
//! );
//! // ... agent runs, inserting records ...
//! handle.stop(); // waits for the task to finish its current loop
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use skeg_rigging::IterVectors;

use crate::Hansa;
use crate::saga::build_saga_from_tenant;

/// Configuration for [`Hansa::start_background_refresh`].
#[derive(Debug, Clone, Copy)]
pub struct BackgroundRefreshConfig {
    /// How often the task wakes to check the record count.
    pub interval: Duration,
    /// Fractional growth that triggers a rebuild. `0.1` = refresh once
    /// the tenant has grown by 10% relative to the last build.
    pub threshold_ratio: f32,
    /// Don't refresh until at least this many *new* records have landed
    /// since the last build. Avoids thrashing on tiny tenants.
    pub min_growth: u64,
    /// Seed for the k-means++ reservoir sampler. Pinning it makes
    /// successive saga rebuilds reproducible in tests.
    pub seed_for_kmeans: u64,
}

impl Default for BackgroundRefreshConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            threshold_ratio: 0.1,
            min_growth: 1,
            seed_for_kmeans: 0,
        }
    }
}

/// Handle to a background refresh task. Stop the task by calling
/// [`Self::stop`] or by letting the handle drop (which signals stop
/// but does not wait for the thread).
pub struct RefreshHandle {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl RefreshHandle {
    /// Signal the task to stop and wait for it to exit.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }

    /// Whether the task is still running.
    pub fn is_running(&self) -> bool {
        !self.stop.load(Ordering::Relaxed)
            && self.join.as_ref().is_some_and(|j| !j.is_finished())
    }
}

impl Drop for RefreshHandle {
    fn drop(&mut self) {
        // Best effort: signal stop. We do not block on join here - that
        // would surprise users who let the handle drop synchronously.
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl<T> Hansa<T>
where
    T: IterVectors + Send + Sync + 'static,
{
    /// Spawn a background thread that rebuilds the local saga when the
    /// tenant grows past the configured threshold.
    ///
    /// `tags_provider` is called every time a rebuild fires; it returns
    /// the current tag stream (rigging v0.1 has no tag-iteration trait,
    /// so the provider is application-supplied). A closure returning
    /// `Vec::new()` is acceptable.
    ///
    /// The task does not panic on rebuild failure: it logs to stderr
    /// and keeps polling. This matches the v0.1 "best effort" model.
    pub fn start_background_refresh<F>(
        &self,
        config: BackgroundRefreshConfig,
        tags_provider: F,
    ) -> RefreshHandle
    where
        F: Fn() -> Vec<String> + Send + Sync + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let tenant = self.local_tenant_arc();
        let tenant_id = self.local_tenant_id();
        let saga_path = self.local_saga_path();
        let initial = tenant.record_count();

        let join = std::thread::spawn(move || {
            let mut last_built = initial;
            loop {
                // Sleep in slices so we react promptly to a stop signal
                // even when `config.interval` is long.
                let slice = Duration::from_millis(50);
                let mut waited = Duration::ZERO;
                while waited < config.interval {
                    if stop_clone.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(slice);
                    waited += slice;
                }
                if stop_clone.load(Ordering::Relaxed) {
                    return;
                }

                let current = tenant.record_count();
                let growth = current.saturating_sub(last_built);
                let threshold = ((last_built as f32) * config.threshold_ratio).ceil() as u64;
                if growth < config.min_growth || growth < threshold {
                    continue;
                }

                let tags = tags_provider();
                let dim = tenant.embedding_dim();
                let vectors: Vec<Vec<f32>> =
                    tenant.iter_vectors().map(|(_, v)| v).collect();
                let now = current_unix_seconds();
                let saga = match build_saga_from_tenant(
                    tenant_id,
                    dim,
                    current,
                    vectors,
                    tags,
                    now,
                    config.seed_for_kmeans,
                ) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("hansa bg refresh: build failed: {e}");
                        continue;
                    }
                };
                if let Err(e) = saga.write_to_path(&saga_path) {
                    eprintln!("hansa bg refresh: write failed: {e}");
                    continue;
                }
                last_built = current;
            }
        });

        RefreshHandle {
            stop,
            join: Some(join),
        }
    }
}

fn current_unix_seconds() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
