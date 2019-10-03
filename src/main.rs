use std::collections::HashMap;

struct Application {
    app_id: String,
    app_name: String,
    timestamp_start: i64,
    timestamp_end: i64,
    user: String,
    spark_version: String,
    queue: Option<String>,
    log_file: String,
    // Event type -> count.
    event_counts_by_type: HashMap<String, u64>,
}

struct Problems {
    app: Application,

    lost_executor_memory_overhead_exceeded: Vec<TaskCountInStage>,
    lost_executor_shell_error: Vec<TaskCountInStage>,
    lost_executor_killed_by_external_signal: Vec<TaskCountInStage>,
    lost_executor_other: Vec<TaskCountInStage>,
    killed_another_attempt_succeeded: Vec<TaskCountInStage>,
    exception_spark_exception: Vec<TaskCountInStage>,
    exception_other: Vec<TaskCountInStage>,

    too_much_gc: Vec<TooMuchGcInStage>,
}

struct TaskCountInStage {
    stage_id: i64,
    matching_count: u64,
    total_count: u64,
}

struct TooMuchGcInStage {
    stage_id: i64,
    selected_task_gc_time_s: f64,
    selected_task_total_time_s: f64,
    substantial_gc_task_count: u64,
    total_task_count: u64,
}

struct Stats {
    cnt: u64,
    percentiles: HashMap<i32, f64>,
    mean: f64,
    std_dev: f64,
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

    Stats {
        cnt: samples.len() as u64,
        percentiles,
        mean,
        std_dev,
    }
}

use std::io::{self, BufRead};

mod parsing;

fn main() {
    let mut applications = Vec::new();
    // Read input file names from stdin.
    for line in io::stdin().lock().lines() {
        let log_file = line.expect("filename from stdin");
        applications.push(parsing::parse_application_log(&log_file));
    }

    // Remove.
    for app in applications {
        println!("application {}", app.app_name);
        println!("queue {:?}", app.queue);
        println!("user {}", app.user);
        println!("spark version {}", app.spark_version);
        println!("num. of stages {}", app.stages.len());
        for (_, stage) in app.stages {
            println!("Stage {} has {} tasks", stage.stage_id, stage.tasks.len());
        }
    }

    // From parsed application log compute statistics.

    // Write web page which shows statistics to stdout.
}
