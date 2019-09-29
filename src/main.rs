use std::collections::HashMap;

struct ApplicationSummary {
    application_id: String,
    application_name: String,
    user: String,
    queue: String,
    total_time_secs: f64,
    stages: Vec<StageSummary>,
    // Event type -> count.
    event_counts_in_log: HashMap<String, u64>,
}

struct StageSummary {
    stage_id: String,

    // Statistics for tasks.
    tasks: TasksInStageSummary,

    // Statistics for whole stage.
    cpu_time_secs: f64,
    total_time_secs: f64,
}

struct TasksInStageSummary {
    shuffle_bytes_read: Stats,
    shuffle_bytes_written: Stats,
    deserialization_time_secs: Stats,
    gc_time_secs: Stats,
    ratio_gc_time_to_total_time: Stats,
    cpu_time_secs: Stats,
    total_time_secs: Stats,

    counts: TaskCountsByTerminationReason,
}

struct TaskCountsByTerminationReason {
    executor_lost_preempted: u64,
    executor_lost_other: u64,
    exception: u64,
    success: u64,
    other: u64,
}

struct Stats {
    cnt: u64,
    percentiles: HashMap<i32, f64>,
    mean: f64,
    std_dev: f64,
    // Number of numbers bigger than mean + 4*sigma
    anomaly_cnt: u64,
}

fn quantile(q: f64, sorted_samples: &Vec<f64>) -> f64 {
    assert!(!sorted_samples.is_empty());
    assert!(q >= 0.0 && q <= 1.0);

    if sorted_samples.len() == 1 {
        return sorted_samples[0];
    }

    let len = sorted_samples.len() as f64;
    let rank = q * (len - 1.0);
    let lo_index = rank as usize;
    let hi_index = lo_index + 1;

    if hi_index < sorted_samples.len() {
        let lo = sorted_samples[lo_index];
        let hi = sorted_samples[hi_index];
        let weight = rank - rank.floor();
        lo + (hi - lo) * weight
    } else {
        // When `q` is `1.0` or very close `hi_index` is too big.
        sorted_samples[lo_index]
    }
}

fn mean(samples: &Vec<f64>) -> f64 {
    assert!(!samples.is_empty());

    let mut sum = 0.0;
    for x in samples {
        sum += *x;
    }

    sum / samples.len() as f64
}

fn std_dev(mean: f64, samples: &Vec<f64>) -> f64 {
    if samples.len() <= 1 {
        return 0.0;
    }

    let mut sq_sum = 0.0;
    for x in samples {
        let z = *x - mean;
        sq_sum += z * z;
    }

    let variance = sq_sum / (samples.len() as f64 - 1.0);
    variance.sqrt()
}

fn compute_stats(mut samples: Vec<f64>) -> Stats {
    assert!(!samples.is_empty());

    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut percentiles = HashMap::new();
    for p in [0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100].iter() {
        percentiles.insert(*p, quantile(*p as f64 / 100.0, &samples));
    }

    let mean = mean(&samples);
    let std_dev = std_dev(mean, &samples);

    let anomaly_threshold = mean + 4.0 * std_dev;
    let mut anomaly_cnt = 0u64;
    for x in samples.iter() {
        if *x >= anomaly_threshold {
            anomaly_cnt += 1;
        }
    }

    Stats {
        cnt: samples.len() as u64,
        percentiles,
        mean,
        std_dev,
        anomaly_cnt,
    }
}

fn main() {
    // Process Spark history folder which contains logs in json lines format.
    // Read events from stdin or read files with events.

    // Process events, gather statistics.
    // Log things which should be processed but weren't to stdout (eg. unknown termination reasons).

    // Write web page which shows statistics to stdout.
}
