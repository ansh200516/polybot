//! Kill switch watcher (spec §15): SIGUSR1 or sentinel file → flag. Manual
//! clear = remove the file and restart (TUI key arrives in M4).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::warn;

pub fn spawn_kill_watch(kill_file: PathBuf, flag: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sig =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1()).ok();
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if kill_file.exists() && !flag.swap(true, Ordering::AcqRel) {
                        warn!("kill switch: sentinel file {} present", kill_file.display());
                    }
                }
                _ = async {
                    match sig.as_mut() {
                        Some(s) => { s.recv().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if !flag.swap(true, Ordering::AcqRel) {
                        warn!("kill switch: SIGUSR1 received");
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn sentinel_file_trips_flag() {
        let dir = tempfile::tempdir().unwrap();
        let kill_file = dir.path().join("KILL");
        let flag = Arc::new(AtomicBool::new(false));
        let handle = spawn_kill_watch(kill_file.clone(), Arc::clone(&flag));

        std::fs::write(&kill_file, b"").unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !flag.load(Ordering::Acquire) {
            if std::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(flag.load(Ordering::Acquire), "sentinel file must trip flag");
        handle.abort();
    }
}
