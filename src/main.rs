use std::collections::{HashMap, HashSet};

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct Application {
    app_id: String,
    app_name: String,
    dt_start: String,
    dt_end: String,
    wallclock_secs: u64,
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

    // Stages which contain tasks longer than 5 minutes
    // where GC takes at least 50 % of task time.
    gc_intensive_tasks: Vec<TaskCountInStage>,
    // If GC takes at least 5 hours and at least 10 % of total time.
    gc_intensive_app: Option<GcIntensiveApp>,
    big_memory_tasks: Vec<BigMemoryTasksInStage>,

    // Stages which contain tasks longer than 1 hour.
    slow_tasks: Vec<TaskCountInStage>,

    // Whether this application has any successful task.
    // No successful task may mean that application is logging
    // only errors and warnings and this report is not reliable.
    successful_task: bool,
}

fn has_problem(problems: &Problems) -> bool {
    problems.lost_executor_memory_overhead_exceeded.len() > 0 ||
        problems.lost_executor_shell_error.len() > 0 ||
        problems.lost_executor_killed_by_external_signal.len() > 0 ||
        problems.lost_executor_other.len() > 0 ||
        problems.killed_another_attempt_succeeded.len() > 0 ||
        problems.exception.len() > 0 ||
        problems.gc_intensive_tasks.len() > 0 ||
        problems.big_memory_tasks.len() > 0 ||
        problems.slow_tasks.len() > 0 ||
        !problems.successful_task
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct TaskCountInStage {
    stage_id: i64,
    matching_count: u64,
    with_metrics_count: u64,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct GcIntensiveApp {
    total_secs_for_all_tasks: u64,
    total_secs_for_gc: u64,
    task_with_metrics_count: u64,
}

// TODO We should also compare whether tasks across different stages
//      use similar amount of memory.
// TODO How does Spark measure memory used by single task?
//      For sure it's possible to measure memory used by executor
//      but if executor executes multiple tasks at once how does
//      Spark measure their memory usage??

// Stages which contain tasks which use twice more memory than quartile 3.
// Stages where every task uses less than 4 GB of memory are ignored.
#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct BigMemoryTasksInStage {
    stage_id: i64,
    peak_memory_usage_quartile3: u64,
    peak_memory_usage_max: u64,
    big_memory_task_count: u64,
    task_with_metrics_count: u64,
    total_task_count: u64,
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

fn percentile(p: i32, sorted_samples: &Vec<f64>) -> f64 {
    quantile(p as f64 / 100.0, sorted_samples)
}

fn sort_floats(floats: &mut Vec<f64>) {
    floats.sort_by(|a, b| a.partial_cmp(b).unwrap());
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

fn timestamp_to_dt(timestamp_ms: i64) -> String {
    let dt = NaiveDateTime::from_timestamp(timestamp_ms / 1000, 0);
    dt.format("%Y-%m-%d %H:%M").to_string()
}

use std::io::{self, BufRead};
use crate::parsing::{ParsedTaskEndReason, ParsedStage, ParsedTask};
use chrono::NaiveDateTime;

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
                with_metrics_count: stage.tasks.len() as u64,
            });
        }
    }

    result.sort_by_key(|x| x.stage_id);
    result
}

fn find_stages_with_gc_intensive_tasks(stages: &HashMap<i64, ParsedStage>) -> Vec<TaskCountInStage> {
    let mut result = Vec::new();

    for (_, stage) in stages.iter() {
        let mut matching_count = 0u64;
        let mut with_metrics_count = 0u64;


        for task in stage.tasks.iter() {
            match task.metrics.as_ref() {
                None => (),
                Some(metrics) => {
                    let gc_secs = metrics.jvm_gc_time as f64 / 1000.0;
                    let total_secs = metrics.executor_run_time as f64 / 1000.0;

                    if total_secs >= 5.0 * 60.0 && gc_secs / total_secs >= 0.5 {
                        matching_count += 1;
                    }
                    with_metrics_count += 1;
                },
            }
        }

        if matching_count > 0 {
            result.push(TaskCountInStage {
                stage_id: stage.stage_id,
                matching_count,
                with_metrics_count,
            });
        }
    }

    result.sort_by_key(|x| x.stage_id);
    result
}

fn detect_gc_intensive_app(stages: &HashMap<i64, ParsedStage>) -> Option<GcIntensiveApp> {
    let mut total_secs_for_gc = 0.0;
    let mut total_secs_for_all_tasks = 0.0;
    let mut task_with_metrics_count = 0u64;

    for (_, stage) in stages.iter() {
        for task in stage.tasks.iter() {
            match task.metrics.as_ref() {
                None => (),
                Some(metrics) => {
                    total_secs_for_gc += metrics.jvm_gc_time as f64 / 1000.0;
                    total_secs_for_all_tasks += metrics.executor_run_time as f64 / 1000.0;
                    task_with_metrics_count += 1;
                },
            }
        }
    }

    if total_secs_for_gc >= 5.0 * 3600.0 && total_secs_for_gc / total_secs_for_all_tasks >= 0.1 {
        Some(GcIntensiveApp {
            total_secs_for_all_tasks: total_secs_for_all_tasks as u64,
            total_secs_for_gc: total_secs_for_gc as u64,
            task_with_metrics_count,
        })
    } else {
        None
    }
}

fn find_stages_with_slow_tasks(stages: &HashMap<i64, ParsedStage>) -> Vec<TaskCountInStage> {
    let mut result = Vec::new();

    for (_, stage) in stages.iter() {
        let mut matching_count = 0u64;
        let mut with_metrics_count = 0u64;


        for task in stage.tasks.iter() {
            match task.metrics.as_ref() {
                None => (),
                Some(metrics) => {
                    let total_secs = metrics.executor_run_time as f64 / 1000.0;

                    if total_secs >= 60.0 * 60.0 {
                        matching_count += 1;
                    }
                    with_metrics_count += 1;
                },
            }
        }

        if matching_count > 0 {
            result.push(TaskCountInStage {
                stage_id: stage.stage_id,
                matching_count,
                with_metrics_count,
            });
        }
    }

    result.sort_by_key(|x| x.stage_id);
    result
}

fn has_successful_task(stages: &HashMap<i64, ParsedStage>) -> bool {
    for (_, stage) in stages.iter() {
        for task in stage.tasks.iter() {
            match task.task_end_reason {
                ParsedTaskEndReason::Success => return true,
                _ => (),
            }
        }
    }
    false
}

fn get_accumulated_value(task: &ParsedTask, accumulable_name: &str) -> Option<u64> {
    let mut values = Vec::new();
    for acc in task.accumulables.iter() {
        if acc.name == accumulable_name {
            match acc.update.as_u64() {
                None => eprintln!("Accumulable {} has value {:?} which is not u64", accumulable_name, acc.update),
                Some(value) => values.push(value),
            }
        }
    }

    values.sort();
    // There may be multiple accumulables with the same name.
    // This is fine if they have the same value.
    if values.first() != values.last() {
        eprintln!("Accumulable {} contains different values {:?}", accumulable_name, values);
    }

    values.last().cloned()
}

fn get_peak_memory_usage(task: &ParsedTask) -> Option<u64> {
    get_accumulated_value(task, "internal.metrics.peakExecutionMemory")
}

fn find_stages_with_big_memory_tasks(stages: &HashMap<i64, ParsedStage>) -> Vec<BigMemoryTasksInStage> {
    let mut result = Vec::new();

    for (_, stage) in stages.iter() {
        let mut peak_memory_usage = Vec::new();

        for task in stage.tasks.iter() {
            match get_peak_memory_usage(task) {
                None => (),
                Some(peak) => peak_memory_usage.push(peak as f64),
            }
        }

        // Warn if more than 25 % of tasks lack peak memory usage --
        // in such case diagnostics may not be precise.
        if peak_memory_usage.len() != 0 && (peak_memory_usage.len() as f64 / stage.tasks.len() as f64) < 0.75 {
            eprintln!("More than 25 % of tasks lack peak memory usage: {}/{}", peak_memory_usage.len(), stage.tasks.len());
        }

        // Ensure that enough tasks in this stage have metrics.
        if peak_memory_usage.len() >= 20 {
            sort_floats(&mut peak_memory_usage);
            let quartile3 = percentile(75, &peak_memory_usage);
            let max = percentile(100, &peak_memory_usage);
            let mut big_memory_task_count = 0u64;
            for usage in peak_memory_usage.iter() {
                if *usage > quartile3 * 2.0 {
                    big_memory_task_count += 1;
                }
            }

            // Ignore stages where tasks use small amount of memory.
            if big_memory_task_count > 0 && max > 4.0 * 1024.0 * 1024.0 * 1024.0 {
                result.push(BigMemoryTasksInStage {
                    stage_id: stage.stage_id,
                    peak_memory_usage_quartile3: quartile3 as u64,
                    peak_memory_usage_max: max as u64,
                    big_memory_task_count,
                    task_with_metrics_count: peak_memory_usage.len() as u64,
                    total_task_count: stage.tasks.len() as u64,
                });

            }
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
            dt_start: timestamp_to_dt(parsed.timestamp_start),
            dt_end: timestamp_to_dt(parsed.timestamp_end),
            wallclock_secs: ((parsed.timestamp_end - parsed.timestamp_start) / 1000) as u64,
            user: parsed.user,
            spark_version: parsed.spark_version,
            queue: parsed.queue,
            log_file,
            event_counts_by_type: parsed.event_counts_by_type,
        };
        eprintln!("Parsed {} ({})", app.app_id, app.dt_start);

        let problems = Problems {
            app,

            lost_executor_memory_overhead_exceeded: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorMemoryOverheadExceeded, &parsed.stages),
            lost_executor_heartbeat_timed_out: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorHeartbeatTimedOut, &parsed.stages),
            lost_executor_shell_error: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorShellError, &parsed.stages),
            lost_executor_killed_by_external_signal: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorKilledByExternalSignal, &parsed.stages),
            lost_executor_other: count_tasks_with_task_end_reason(ParsedTaskEndReason::LostExecutorOther, &parsed.stages),
            killed_another_attempt_succeeded: count_tasks_with_task_end_reason(ParsedTaskEndReason::KilledAnotherAttemptSucceeded, &parsed.stages),
            exception: count_tasks_with_task_end_reason(ParsedTaskEndReason::Exception, &parsed.stages),

            gc_intensive_tasks: find_stages_with_gc_intensive_tasks(&parsed.stages),
            gc_intensive_app: detect_gc_intensive_app(&parsed.stages),
            big_memory_tasks: find_stages_with_big_memory_tasks(&parsed.stages),

            slow_tasks: find_stages_with_slow_tasks(&parsed.stages),
            successful_task: has_successful_task(&parsed.stages),
        };

        if has_problem(&problems) {
            report.push(problems);
        }
    }

    let report_json = serde_json::to_string_pretty(&report).expect("serialized JSON");

    let report_html_template = include_str!("report.html");
    let report_html = report_html_template.replace("\"{REPORT}\"", &report_json);

    println!("{}", report_html);
}
