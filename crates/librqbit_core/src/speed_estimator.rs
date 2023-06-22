use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;

#[derive(Clone, Copy)]
pub struct ProgressSnapshot {
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub instant: Instant,
}

pub struct SpeedEstimator {
    latest_per_second_snapshots: Mutex<VecDeque<ProgressSnapshot>>,
    download_bytes_per_second: AtomicU64,
    upload_bytes_per_second: AtomicU64,
    time_remaining_millis: AtomicU64,
}

impl SpeedEstimator {
    pub fn new(window_seconds: usize) -> Self {
        assert!(window_seconds > 1);
        Self {
            latest_per_second_snapshots: Mutex::new(VecDeque::with_capacity(window_seconds)),
            download_bytes_per_second: Default::default(),
            upload_bytes_per_second: Default::default(),
            time_remaining_millis: Default::default(),
        }
    }

    pub fn time_remaining(&self) -> Option<Duration> {
        let tr = self.time_remaining_millis.load(Ordering::Relaxed);
        if tr == 0 {
            return None;
        }
        Some(Duration::from_millis(tr))
    }

    pub fn download_bps(&self) -> u64 {
        self.download_bytes_per_second.load(Ordering::Relaxed)
    }

    pub fn upload_bps(&self) -> u64 {
        self.upload_bytes_per_second.load(Ordering::Relaxed)
    }

    pub fn download_mbps(&self) -> f64 {
        self.download_bps() as f64 / 1024f64 / 1024f64
    }

    pub fn snapshots(&self) -> VecDeque<ProgressSnapshot> {
        self.latest_per_second_snapshots.lock().clone()
    }

    pub fn add_snapshot(
        &self,
        uploaded_bytes: u64,
        downloaded_bytes: u64,
        remaining_bytes: u64,
        instant: Instant,
    ) {
        let first = {
            let mut g = self.latest_per_second_snapshots.lock();

            let current = ProgressSnapshot {
                uploaded_bytes,
                downloaded_bytes,
                instant,
            };

            if g.is_empty() {
                g.push_back(current);
                return;
            } else if g.len() < g.capacity() {
                g.push_back(current);
                g.front().copied().unwrap()
            } else {
                let first = g.pop_front().unwrap();
                g.push_back(current);
                first
            }
        };

        let uploaded_bytes_diff = uploaded_bytes - first.uploaded_bytes;
        let downloaded_bytes_diff = downloaded_bytes - first.downloaded_bytes;
        let elapsed = instant - first.instant;
        let bps = downloaded_bytes_diff as f64 / elapsed.as_secs_f64();
        let ubps = uploaded_bytes_diff as f64 / elapsed.as_secs_f64();

        let time_remaining_millis_rounded: u64 = if downloaded_bytes_diff > 0 {
            let time_remaining_secs = remaining_bytes as f64 / bps;
            (time_remaining_secs * 1000f64) as u64
        } else {
            0
        };
        self.time_remaining_millis
            .store(time_remaining_millis_rounded, Ordering::Relaxed);
        self.download_bytes_per_second
            .store(bps as u64, Ordering::Relaxed);
        self.upload_bytes_per_second
            .store(ubps as u64, Ordering::Relaxed);
    }
}
