use crate::core::types::Record;
use std::time::{Duration, Instant};

#[macro_export]
macro_rules! time_record_processing {
    ($stage:expr, $record:expr, $code:expr) => {{
        let start = std::time::Instant::now();
        let result = $code;
        $record.mark_timestamp($stage);
        log::debug!(
            "Record processed by {} in {:.6}s",
            $stage,
            start.elapsed().as_secs_f64()
        );
        result
    }};

    ($stage:expr, $records:expr, $code:expr) => {{
        let start = std::time::Instant::now();
        let result = $code;
        for record in $records.iter_mut() {
            record.mark_timestamp($stage);
        }
        log::debug!(
            "{} records processed by {} in {:.6}s",
            $records.len(),
            $stage,
            start.elapsed().as_secs_f64()
        );
        result
    }};
}

pub fn process_records_with_timing<F, R>(stage_name: &str, records: &mut [Record], f: F) -> R
where
    F: FnOnce(&mut [Record]) -> R,
{
    let start = Instant::now();
    let result = f(records);
    let elapsed = start.elapsed();
    let len = records.len();

    for record in records {
        record.mark_timestamp(stage_name);
    }

    log::debug!(
        "{} records processed by {} in {:.6}s",
        len,
        stage_name,
        elapsed.as_secs_f64()
    );

    result
}

pub fn process_record_with_timing<F, R>(stage_name: &str, record: &mut Record, f: F) -> R
where
    F: FnOnce(&mut Record) -> R,
{
    let start = Instant::now();
    let result = f(record);
    let elapsed = start.elapsed();

    record.mark_timestamp(stage_name);

    log::debug!(
        "Record processed by {} in {:.6}s",
        stage_name,
        elapsed.as_secs_f64()
    );

    result
}

pub fn summarize_record_timings(record: &Record) -> String {
    let timestamps = record.get_stage_duration();
    if timestamps.is_empty() {
        return "No timing information available".to_string();
    }

    let mut stages: Vec<_> = timestamps.iter().collect();
    stages.sort_by(|(_, a), (_, b)| a.cmp(b));

    let mut summary = String::from("Record processing timeline:\n");
    summary.push_str(&format!(
        "  Total: {:.6}s\n",
        record.creation_time().elapsed().as_secs_f64()
    ));
    let mut prev_time = Duration::from_secs(0);

    for (stage, duration) in stages {
        let stage_duration = if prev_time.as_nanos() > 0 {
            duration.saturating_sub(prev_time)
        } else {
            *duration
        };

        summary.push_str(&format!(
            "  {}: +{:.6}s (total: {:.6}s)\n",
            stage,
            stage_duration.as_secs_f64(),
            duration.as_secs_f64()
        ));

        prev_time = *duration;
    }

    summary
}

pub fn mark_pipeline_stage(records: &mut [Record], stage: &str) {
    for record in records.iter_mut() {
        record.mark_timestamp(stage);
    }

    let len = records.len();

    if !records.is_empty() {
        let timestamp = records
            .get(0)
            .expect("Records should not be empty")
            .get_timestamp(stage)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        log::debug!(
            "Marked {} records at stage '{}', elapsed time: {:.6}s",
            len,
            stage,
            timestamp
        );
    }
}
