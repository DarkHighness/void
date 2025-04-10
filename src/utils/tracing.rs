use std::{fmt::Display, sync::Arc};

use dashmap::DashMap;

use crate::{config::global::use_time_tracing, core::tag::TagId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    None,
    Incoming,
    Outgoing,
}

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::None => write!(f, "none"),
            Direction::Incoming => write!(f, "incoming"),
            Direction::Outgoing => write!(f, "outgoing"),
        }
    }
}

#[derive(Debug, Clone)]

pub struct Timepoint {
    pub stage: TagId,
    pub time: std::time::Instant,
    pub direction: Direction,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeRangeKey {
    pub from: (TagId, Direction),
    pub to: (TagId, Direction),
}

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub key: String,
    pub elapsed: std::time::Duration,
}

#[derive(Debug)]

pub struct TracingContext {
    timepoints: spin::Mutex<Vec<Timepoint>>,
    parent: Option<Arc<TracingContext>>,
}

impl TracingContext {
    pub fn new_root() -> Arc<Self> {
        let ctx = TracingContext {
            timepoints: spin::Mutex::new(Vec::new()),
            parent: None,
        };

        Arc::new(ctx)
    }
    pub fn inherit(parent: Arc<TracingContext>) -> Arc<Self> {
        let ctx = TracingContext {
            timepoints: spin::Mutex::new(Vec::new()),
            parent: Some(parent),
        };

        Arc::new(ctx)
    }

    pub fn add_timepoint(&self, stage: &TagId, direction: Direction) {
        let timepoint = Timepoint {
            stage: stage.clone(),
            time: std::time::Instant::now(),
            direction,
        };

        let mut timepoints = self.timepoints.lock();
        timepoints.push(timepoint);
    }

    pub fn record(&self) {
        if !use_time_tracing() {
            return;
        }

        let mut parent = self.parent.clone();
        let mut timepoints = self.timepoints.lock().clone();

        while let Some(p) = parent {
            {
                let parent_timepoints = p.timepoints.lock();
                parent_timepoints
                    .iter()
                    .for_each(|e| timepoints.push(e.clone()));
            }

            parent = p.parent.clone();
        }

        timepoints.sort_by(|a, b| a.time.cmp(&b.time));
        if timepoints.len() < 2 {
            return;
        }

        let start_timestamp = timepoints
            .first()
            .map(|e| e.time)
            .unwrap_or(std::time::Instant::now());

        for (i, item) in timepoints.iter().enumerate() {
            let elapsed = item.time.duration_since(start_timestamp);
            let key = format!("[{}] {}({})", i, item.stage, item.direction);
            let range = TimeRange { key, elapsed };

            GLOBAL_TRACING.add_time_range(range);
        }
    }
}

#[derive(Debug)]
pub struct GlobalTracing {
    window_interval: std::time::Duration,
    buffer: DashMap<String, Vec<std::time::Duration>>,
}

impl GlobalTracing {
    pub fn new() -> Self {
        Self {
            window_interval: std::time::Duration::from_secs(10),
            buffer: DashMap::new(),
        }
    }

    pub fn add_time_range(&self, range: TimeRange) {
        if !use_time_tracing() {
            return;
        }

        let mut entry = self.buffer.entry(range.key).or_default();
        entry.push(range.elapsed);
    }

    fn summary(&self) {
        if !use_time_tracing() {
            return;
        }

        // mean, stddev, min, max, p25, p50, p75, p90 by key
        let mut summary = vec![];

        for mut entry in self.buffer.iter_mut() {
            let key = entry.key().clone();
            let elapsed = entry.value_mut();
            elapsed.sort();

            let count = elapsed.len() as i64;
            let mean = elapsed.iter().copied().sum::<std::time::Duration>() / elapsed.len() as u32;
            let min = *elapsed.iter().min().unwrap_or(&std::time::Duration::ZERO);
            let max = *elapsed.iter().max().unwrap_or(&std::time::Duration::ZERO);
            let p25 = elapsed[elapsed.len() / 4];
            let p50 = elapsed[elapsed.len() / 2];
            let p75 = elapsed[3 * elapsed.len() / 4];
            let p90 = elapsed[9 * elapsed.len() / 10];

            let mean = mean.as_millis() as i64;
            let min = min.as_millis() as i64;
            let max = max.as_millis() as i64;
            let p25 = p25.as_millis() as i64;
            let p50 = p50.as_millis() as i64;
            let p75 = p75.as_millis() as i64;
            let p90 = p90.as_millis() as i64;

            summary.push((key, count, mean, min, max, p25, p50, p75, p90));
        }

        summary.sort_by(|a, b| a.0.cmp(&b.0));
        eprintln!("Time Tracing Summary:");
        eprintln!("=========================");
        eprintln!("| Key | Count | Mean (ms) | Min (ms) | Max (ms) | P25 (ms) | P50 (ms) | P75 (ms) | P90 (ms) |");
        eprintln!("-------------------------------------------------");
        for (key, count, mean, min, max, p25, p50, p75, p90) in summary.iter() {
            eprintln!(
                "{:40} | {:6} | {:+6} | {:+6} | {:+6} | {:+6} | {:+6} | {:+6} | {:+6}",
                key, count, mean, min, max, p25, p50, p75, p90
            );
        }
        eprintln!("-------------------------------------------------");
    }

    fn clear(&self) {
        if !use_time_tracing() {
            return;
        }

        self.buffer.clear();
    }
}

pub static GLOBAL_TRACING: once_cell::sync::Lazy<Arc<GlobalTracing>> =
    once_cell::sync::Lazy::new(|| Arc::new(GlobalTracing::new()));

pub fn spawn_tracing_task() {
    if !use_time_tracing() {
        return;
    }

    let global_tracing = GLOBAL_TRACING.clone();
    tokio::task::Builder::new()
        .name("tracing")
        .spawn(async move {
            loop {
                tokio::time::sleep(global_tracing.window_interval).await;
                global_tracing.summary();
                global_tracing.clear();
            }
        })
        .expect("Failed to spawn tracing task");
}
