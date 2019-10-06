use std::collections::HashMap;

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
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

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct Problems {
    app: Application,

    lost_executor_memory_overhead_exceeded: Vec<TaskCountInStage>,
    lost_executor_heartbeat_timed_out: Vec<TaskCountInStage>,
    lost_executor_shell_error: Vec<TaskCountInStage>,
    lost_executor_killed_by_external_signal: Vec<TaskCountInStage>,
    lost_executor_other: Vec<TaskCountInStage>,
    killed_another_attempt_succeeded: Vec<TaskCountInStage>,
    exception: Vec<TaskCountInStage>,

    too_much_gc: Vec<TooMuchGcInStage>,
}

fn has_problem(problems: &Problems) -> bool {
    problems.lost_executor_memory_overhead_exceeded.len() > 0 ||
        problems.lost_executor_shell_error.len() > 0 ||
        problems.lost_executor_killed_by_external_signal.len() > 0 ||
        problems.lost_executor_other.len() > 0 ||
        problems.killed_another_attempt_succeeded.len() > 0 ||
        problems.exception.len() > 0 ||
        problems.too_much_gc.len() > 0
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct TaskCountInStage {
    stage_id: i64,
    matching_count: u64,
    total_count: u64,
}

// Stages where GC takes more than 20 % of time.
// Stages shorter than 2 minutes are ignored.
#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct TooMuchGcInStage {
    stage_id: i64,
    gc_secs_for_all_tasks: f64,
    total_secs_for_all_tasks: f64,
    task_with_metrics_count: u64,
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
use crate::parsing::{ParsedTaskEndReason, ParsedStage};

mod parsing;

// TODO Stage_id is same in different attempts?? If so we should use pair (stage_id, stage_attempt_id).

fn count_tasks_with_task_end_reason(task_end_reason: ParsedTaskEndReason, stages: &HashMap<i64, ParsedStage>) -> Vec<TaskCountInStage> {
    let mut result = Vec::new();

    for (_, stage) in stages.iter() {
        let mut matching_count = 0u64;
        for task in stage.tasks.iter() {
            if task.task_end_reason == task_end_reason {
                matching_count += 1;
            }
        }

        if matching_count > 0 {
            result.push(TaskCountInStage {
                stage_id: stage.stage_id,
                matching_count,
                total_count: stage.tasks.len() as u64,
            });
        }
    }

    result.sort_by_key(|x| x.stage_id);
    result
}

fn find_stages_with_too_much_gc(stages: &HashMap<i64, ParsedStage>) -> Vec<TooMuchGcInStage> {
    let mut result = Vec::new();

    for (_, stage) in stages.iter() {
        let mut gc_secs_for_all_tasks = 0f64;
        let mut total_secs_for_all_tasks = 0f64;
        let mut task_with_metrics_count = 0u64;


        for task in stage.tasks.iter() {
            match task.metrics.as_ref() {
                None => (),
                Some(metrics) => {
                    gc_secs_for_all_tasks += metrics.jvm_gc_time as f64 / 1000.0;
                    total_secs_for_all_tasks += metrics.executor_run_time as f64 / 1000.0;
                    task_with_metrics_count += 1;
                },
            }
        }

        if total_secs_for_all_tasks >= 120.0 && (gc_secs_for_all_tasks / total_secs_for_all_tasks) > 0.2 {
            result.push(TooMuchGcInStage {
                stage_id: stage.stage_id,
                gc_secs_for_all_tasks,
                total_secs_for_all_tasks,
                task_with_metrics_count
            });
        }
    }

    result.sort_by_key(|x| x.stage_id);
    result
}

fn main() {
    let mut report = Vec::new();
    // Read input file names from stdin.
    for line in io::stdin().lock().lines() {
        let log_file = line.expect("filename from stdin");
        let parsed = parsing::parse_application_log(&log_file);

        let app = Application {
            app_id: parsed.app_id,
            app_name: parsed.app_name,
            timestamp_start: parsed.timestamp_start,
            timestamp_end: parsed.timestamp_end,
            user: parsed.user,
            spark_version: parsed.spark_version,
            queue: parsed.queue,
            log_file,
            event_counts_by_type: parsed.event_counts_by_type,
        };

        let problems = Problems {
            app,

            lost_executor_memory_overhead_exceeded: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorMemoryOverheadExceeded, &parsed.stages),
            lost_executor_heartbeat_timed_out: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorHeartbeatTimedOut, &parsed.stages),
            lost_executor_shell_error: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorShellError, &parsed.stages),
            lost_executor_killed_by_external_signal: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorKilledByExternalSignal, &parsed.stages),
            lost_executor_other: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorOther, &parsed.stages),
            killed_another_attempt_succeeded: count_tasks_with_task_end_reason(ParsedTaskEndReason::KilledAnotherAttemptSucceeded, &parsed.stages),
            exception: count_tasks_with_task_end_reason(ParsedTaskEndReason::Exception, &parsed.stages),

            too_much_gc: find_stages_with_too_much_gc(&parsed.stages),
        };

        if has_problem(&problems) {
            report.push(problems);
        }
    }

    for item in report.iter_mut() {
        item.app.event_counts_by_type.clear();
        println!("{:?}", item);
    }

    // Write web page which shows statistics to stdout.
}
