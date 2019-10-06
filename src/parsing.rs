
use std::env;
use std::fs;
use serde::{Serialize, Deserialize};
use std::time::SystemTime;
use std::path::Path;
use std::io::BufReader;
use std::error::Error;
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};

// Initially generated by https://typegen.vestera.as/

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
struct SparkListenerTaskEndEvent {
    #[serde(rename = "Event")]
    event: String,
    #[serde(rename = "Stage ID")]
    stage_id: i64,
    #[serde(rename = "Stage Attempt ID")]
    stage_attempt_id: i64,
    #[serde(rename = "Task Type")]
    task_type: String,
    #[serde(rename = "Task End Reason")]
    task_end_reason: TaskEndReason,
    #[serde(rename = "Task Info")]
    task_info: TaskInfo,
    #[serde(rename = "Task Metrics")]
    task_metrics: Option<TaskMetrics>,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskEndReason {
    #[serde(rename = "Reason")]
    reason: String,

    // Present when reason is `ExecutorLostFailure`.
    #[serde(rename = "Executor ID")]
    executor_id: Option<String>,
    #[serde(rename = "Exit Caused By App")]
    exit_caused_by_app: Option<bool>,
    #[serde(rename = "Loss Reason")]
    loss_reason: Option<String>,

    // Present when reason is `ExceptionFailure`.
    #[serde(rename = "Class Name")]
    class_name: Option<String>,
    #[serde(rename = "Description")]
    description: Option<String>,
    #[serde(rename = "Full Stack Trace")]
    full_stack_trace: Option<String>,
    #[serde(rename = "Stack Trace")]
    stack_trace: Option<serde_json::Value>,

    // Present when reason is `TaskKilled`.
    #[serde(rename = "Kill Reason")]
    kill_reason: Option<String>,

    // Present when reason is `FetchFailed`.
    #[serde(rename = "Block Manager Address")]
    block_manager_address: Option<serde_json::Value>,
    #[serde(rename = "Message")]
    message: Option<String>,
    #[serde(rename = "Map ID")]
    map_id: Option<i64>,
    #[serde(rename = "Reduce ID")]
    reduce_id: Option<i64>,
    #[serde(rename = "Shuffle ID")]
    shuffle_id: Option<i64>,

    #[serde(rename = "Accumulator Updates")]
    accumulator_updates: Option<serde_json::Value>,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskInfo {
    #[serde(rename = "Task ID")]
    task_id: i64,
    #[serde(rename = "Index")]
    index: i64,
    #[serde(rename = "Attempt")]
    attempt: i64,
    #[serde(rename = "Launch Time")]
    launch_time: i64,
    #[serde(rename = "Executor ID")]
    executor_id: String,
    #[serde(rename = "Host")]
    host: String,
    #[serde(rename = "Locality")]
    locality: String,
    #[serde(rename = "Speculative")]
    speculative: bool,
    #[serde(rename = "Getting Result Time")]
    getting_result_time: i64,
    #[serde(rename = "Finish Time")]
    finish_time: i64,
    #[serde(rename = "Failed")]
    failed: bool,
    #[serde(rename = "Killed")]
    killed: bool,
    #[serde(rename = "Accumulables")]
    accumulables: Vec<Accumulable>,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
struct Accumulable {
    #[serde(rename = "ID")]
    id: i64,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Update")]
    update: serde_json::Value,
    #[serde(rename = "Value")]
    value: serde_json::Value,
    #[serde(rename = "Internal")]
    internal: bool,
    #[serde(rename = "Count Failed Values")]
    count_failed_values: bool,
    #[serde(rename = "Metadata")]
    metadata: Option<serde_json::Value>,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskMetrics {
    #[serde(rename = "Executor Deserialize Time")]
    pub executor_deserialize_time: i64,
    #[serde(rename = "Executor Deserialize CPU Time")]
    pub executor_deserialize_cpu_time: i64,
    #[serde(rename = "Executor Run Time")]
    pub executor_run_time: i64,
    #[serde(rename = "Executor CPU Time")]
    pub executor_cpu_time: i64,
    #[serde(rename = "Result Size")]
    pub result_size: i64,
    #[serde(rename = "JVM GC Time")]
    pub jvm_gc_time: i64,
    #[serde(rename = "Result Serialization Time")]
    pub result_serialization_time: i64,
    #[serde(rename = "Memory Bytes Spilled")]
    pub memory_bytes_spilled: i64,
    #[serde(rename = "Disk Bytes Spilled")]
    pub disk_bytes_spilled: i64,
    #[serde(rename = "Shuffle Read Metrics")]
    pub shuffle_read_metrics: ShuffleReadMetrics,
    #[serde(rename = "Shuffle Write Metrics")]
    pub shuffle_write_metrics: ShuffleWriteMetrics,
    #[serde(rename = "Input Metrics")]
    pub input_metrics: InputMetrics,
    #[serde(rename = "Output Metrics")]
    pub output_metrics: OutputMetrics,
    #[serde(rename = "Updated Blocks")]
    pub updated_blocks: Vec<::serde_json::Value>,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShuffleReadMetrics {
    #[serde(rename = "Remote Blocks Fetched")]
    pub remote_blocks_fetched: i64,
    #[serde(rename = "Local Blocks Fetched")]
    pub local_blocks_fetched: i64,
    #[serde(rename = "Fetch Wait Time")]
    pub fetch_wait_time: i64,
    #[serde(rename = "Remote Bytes Read")]
    pub remote_bytes_read: i64,
    // Optional because it's not present in Spark 2.2.
    #[serde(rename = "Remote Bytes Read To Disk")]
    pub remote_bytes_read_to_disk: Option<i64>,
    #[serde(rename = "Local Bytes Read")]
    pub local_bytes_read: i64,
    #[serde(rename = "Total Records Read")]
    pub total_records_read: i64,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShuffleWriteMetrics {
    #[serde(rename = "Shuffle Bytes Written")]
    pub shuffle_bytes_written: i64,
    #[serde(rename = "Shuffle Write Time")]
    pub shuffle_write_time: i64,
    #[serde(rename = "Shuffle Records Written")]
    pub shuffle_records_written: i64,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputMetrics {
    #[serde(rename = "Bytes Read")]
    pub bytes_read: i64,
    #[serde(rename = "Records Read")]
    pub records_read: i64,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputMetrics {
    #[serde(rename = "Bytes Written")]
    pub bytes_written: i64,
    #[serde(rename = "Records Written")]
    pub records_written: i64,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct SparkListenerLogStartEvent {
    #[serde(rename = "Event")]
    event: String,
    #[serde(rename = "Spark Version")]
    spark_version: String,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct SparkListenerEnvironmentUpdateEvent {
    #[serde(rename = "Event")]
    event: String,
    #[serde(rename = "Spark Properties")]
    spark_properties: SparkProperties,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct SparkProperties {
    #[serde(rename = "spark.yarn.queue")]
    spark_yarn_queue: Option<String>
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
struct SparkListenerApplicationStartEvent {
    #[serde(rename = "Event")]
    event: String,
    #[serde(rename = "App Name")]
    app_name: String,
    #[serde(rename = "App ID")]
    app_id: String,
    #[serde(rename = "Timestamp")]
    timestamp: i64,
    #[serde(rename = "User")]
    user: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedApplicationLog {
    pub app_name: String,
    pub app_id: String,
    pub timestamp: i64,
    pub user: String,
    pub spark_version: String,
    pub queue: Option<String>,
    pub stages: HashMap<i64, ParsedStage>,
    pub event_counts_by_type: HashMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedStage {
    pub stage_id: i64,
    pub tasks: Vec<ParsedTask>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedTask {
    pub task_end_reason: ParsedTaskEndReason,
    // Preempted tasks don't have metrics.
    pub metrics: Option<TaskMetrics>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedTaskEndReason {
    Success,
    LostExecutorPreempted,
    LostExecutorMemoryOverheadExceeded,
    LostExecutorHeartbeatTimedOut,
    // Error when executing container from shell. Maybe bad JVM args.
    LostExecutorShellError,
    // Probably memory problem. See YARN log for the container for more details.
    LostExecutorKilledByExternalSignal,
    LostExecutorOther,
    // For speculative execution.
    KilledAnotherAttemptSucceeded,
    KilledCanceled,
    KilledOther,
    Exception,
    Other,
}

fn check_exit_caused_by_app(expected: Option<bool>, e: &SparkListenerTaskEndEvent) {
    if e.task_end_reason.exit_caused_by_app != expected {
        eprintln!("Expected task end reason {:?}: {:?}", expected, e);
    }
}

fn handle_event_spark_listener_task_end(json: serde_json::Value, parsed: &mut ParsedApplicationLog) {
    match serde_json::from_value::<SparkListenerTaskEndEvent>(json.clone()) {
        Ok(e) => {
            if e.task_type != "ResultTask" && e.task_type != "ShuffleMapTask" {
                eprintln!("Unknown task type: {:?}", e);
            }
            let task_end_reason = match e.task_end_reason.reason.as_str() {
                "Success" => {
                    check_exit_caused_by_app(None, &e);
                    ParsedTaskEndReason::Success
                },
                "ExecutorLostFailure" => {
                    let loss_reason = e.task_end_reason.loss_reason.as_ref().expect("executor loss reason");
                    if loss_reason.ends_with(" was preempted.") {
                        check_exit_caused_by_app(Some(false), &e);
                        ParsedTaskEndReason::LostExecutorPreempted
                    } else if loss_reason.starts_with("Container killed by YARN for exceeding memory limits.") &&
                        loss_reason.contains("Consider boosting spark.yarn.executor.memoryOverhead") {
                        check_exit_caused_by_app(Some(true), &e);
                        ParsedTaskEndReason::LostExecutorMemoryOverheadExceeded
                    } else if loss_reason.starts_with("Executor heartbeat timed out after ") {
                        check_exit_caused_by_app(Some(true), &e);
                        ParsedTaskEndReason::LostExecutorHeartbeatTimedOut
                    } else if loss_reason.starts_with("Container marked as failed:") &&
                        loss_reason.contains("org.apache.hadoop.util.Shell.runCommand") {
                        check_exit_caused_by_app(Some(true), &e);
                        ParsedTaskEndReason::LostExecutorShellError
                    } else if loss_reason.starts_with("Container marked as failed:") &&
                        loss_reason.contains("Killed by external signal") {
                        check_exit_caused_by_app(Some(true), &e);
                        ParsedTaskEndReason::LostExecutorKilledByExternalSignal
                    } else if loss_reason.starts_with("Unable to create executor due to Unable to register with external shuffle server due to") ||
                        loss_reason == "Slave lost" {
                        ParsedTaskEndReason::LostExecutorOther
                    } else {
                        eprintln!("Unknown executor loss reason: {:?}", e);
                        ParsedTaskEndReason::LostExecutorOther
                    }
                },
                "ExceptionFailure" => {
                    check_exit_caused_by_app(None, &e);
                    ParsedTaskEndReason::Exception
                },
                "TaskKilled" => {
                    check_exit_caused_by_app(None, &e);
                    let kill_reason = e.task_end_reason.kill_reason.as_ref().expect("kill reason");
                    if kill_reason == "another attempt succeeded" {
                        ParsedTaskEndReason::KilledAnotherAttemptSucceeded
                    } else if kill_reason == "Stage cancelled" {
                        ParsedTaskEndReason::KilledCanceled
                    } else {
                        eprintln!("Unknown kill reason: {:?}", e);
                        ParsedTaskEndReason::KilledOther
                    }
                },
                "FetchFailed" => ParsedTaskEndReason::Other,
                _ => {
                    eprintln!("Unknown task end reason: {:?}", e);
                    ParsedTaskEndReason::Other
                },
            };

            let stage_id = e.stage_id;

            let stage = match parsed.stages.entry(stage_id) {
                Vacant(entry) => entry.insert(ParsedStage {
                    stage_id,
                    tasks: Vec::new(),
                }),
                Occupied(entry) => entry.into_mut(),
            };

            if task_end_reason == ParsedTaskEndReason::Success && e.task_metrics.is_none() {
                eprintln!("Successful task without metrics: {:?}", e);
            }

            stage.tasks.push(ParsedTask {
                task_end_reason,
                metrics: e.task_metrics,
            });
        },
        Err(e) => eprintln!("Cannot parse SparkListenerTaskEndEvent from JSON: {}, {:?}", e, json)
    }
}

fn handle_event_spark_listener_log_start(json: serde_json::Value, parsed: &mut ParsedApplicationLog) {
    let e = serde_json::from_value::<SparkListenerLogStartEvent>(json).expect("JSON representing SparkListenerLogStartEvent");
    parsed.spark_version = e.spark_version;
}

fn handle_event_spark_listener_environment_update(json: serde_json::Value, parsed: &mut ParsedApplicationLog) {
    let e = serde_json::from_value::<SparkListenerEnvironmentUpdateEvent>(json).expect("JSON representing SparkListenerEnvironmentUpdateEvent");
    match e.spark_properties.spark_yarn_queue {
        Some(q) => parsed.queue = Some(q),
        None => (),
    }
}

fn handle_event_spark_listener_application_start(json: serde_json::Value, parsed: &mut ParsedApplicationLog) {
    let e = serde_json::from_value::<SparkListenerApplicationStartEvent>(json).expect("JSON representing SparkListenerApplicationStartEvent");
    parsed.app_name = e.app_name;
    parsed.app_id = e.app_id;
    parsed.timestamp = e.timestamp;
    parsed.user = e.user;
}

pub fn parse_application_log(log_file: &str) -> ParsedApplicationLog {
    let file = fs::File::open(log_file).unwrap();
    let reader = BufReader::new(file);

    let deserializer = serde_json::Deserializer::from_reader(reader);
    let iterator = deserializer.into_iter::<serde_json::Value>();

    let mut parsed = ParsedApplicationLog {
        app_name: String::new(),
        app_id: String::new(),
        timestamp: -1,
        user: String::new(),
        spark_version: String::new(),
        queue: None,
        stages: HashMap::new(),
        event_counts_by_type: HashMap::new(),
    };

    for item in iterator {
        let json = match item {
            Ok(json) => json,
            Err(err) => {
                eprintln!("Cannot parse JSON in log {}: {:?}", log_file, err);
                break
            },
        };
        // Every event has event type.
        let event_type = String::from(json.as_object().expect("root object").get("Event").expect("field Event").as_str().expect("Event string"));

        let entry = match parsed.event_counts_by_type.entry(event_type.clone()) {
            Vacant(entry) => entry.insert(0),
            Occupied(entry) => entry.into_mut(),
        };
        *entry += 1;

        match event_type.as_str() {
            "SparkListenerTaskEnd" => handle_event_spark_listener_task_end(json, &mut parsed),
            "SparkListenerLogStart" => handle_event_spark_listener_log_start(json, &mut parsed),
            "SparkListenerEnvironmentUpdate" => handle_event_spark_listener_environment_update(json, &mut parsed),
            "SparkListenerApplicationStart" => handle_event_spark_listener_application_start(json, &mut parsed),
            _ => (),
        }
    };

    parsed
}
