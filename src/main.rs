use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

type SerdeObject = Map<String, Value>;

fn main() {
    let mut args = std::env::args();

    let path = args
        .nth(1)
        .expect("first argument to be a file path like '/home/user/'");
    let write_unique = args.nth(0).unwrap_or_else(|| "false".to_string());

    let write_unique = match write_unique.as_str() {
        "true" => true,
        "false" => false,
        _ => panic!("Expected second argument to be 'true' or 'false'"),
    };

    let start = std::time::Instant::now();
    let mut files = Vec::new();
    craw_path_for_metric_files(Path::new(&path), &mut files);
    println!("Took {:?} to crawl", start.elapsed());

    println!("Extracting events");
    let start = std::time::Instant::now();
    let events = extract_events(&files);
    println!("Took {:?} to extract events", start.elapsed());

    println!("Filtering metric events");
    let start = std::time::Instant::now();
    let metrics = filter_metric_events(&events);
    println!("Took {:?} to filter metric events", start.elapsed());

    println!("Getting unique metrics");
    let start = std::time::Instant::now();
    let unique_metrics = get_unique_metrics(&metrics);
    println!("Took {:?} to get unique metrics", start.elapsed());

    println!("Counting metric types");
    let start = std::time::Instant::now();
    let metric_types = count_metric_types(&metrics);
    println!("Took {:?} to count metric types", start.elapsed());

    println!("Total number of files: {}", files.len());
    println!("Total number of events: {}", events.len());
    println!("Total number of metrics: {}", metrics.len());
    println!("Total number of unique metrics: {}", unique_metrics.len());

    println!("Metric types:");
    for (metric_type, count) in metric_types {
        println!("  {}: {}", metric_type, count);
    }

    if write_unique {
        write_metrics(
            "unique_metrics.txt",
            &unique_metrics.into_iter().collect::<Vec<_>>(),
        );
    }
}

fn craw_path_for_metric_files(path: &Path, files: &mut Vec<PathBuf>) {
    if path.is_dir() {
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(err) => {
                panic!("Failed to read dir {}: {}", path.display(), err);
            }
        };

        for entry in entries.flatten() {
            craw_path_for_metric_files(&entry.path(), files);
        }
    } else if path.is_file() {
        let file_name = path.file_name().unwrap().to_str().unwrap();

        if !(file_name.starts_with("metrics") && file_name.ends_with(".out")) {
            return;
        }

        files.push(path.to_path_buf());
    }
}

fn extract_events(paths: &[PathBuf]) -> Vec<SerdeObject> {
    let mut events = Vec::new();

    for path in paths {
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(err) => {
                panic!("Failed to read file {:?}: {}", path, err);
            }
        };

        for line in contents.lines() {
            match serde_json::from_str(&line) {
                Ok(Value::Object(event)) => events.push(event),
                Ok(json) => {
                    println!("Ignoring non-object: {:?}", json);
                    continue;
                }
                Err(err) => {
                    panic!("Failed to parse file {:?}: {}", path, err);
                }
            };
        }
    }

    events
}

fn filter_metric_events(events: &[SerdeObject]) -> Vec<SerdeObject> {
    let mut metrics = Vec::new();

    for event in events {
        if let Some(metric) = event.get("metric") {
            metrics.push(
                metric
                    .as_object()
                    .expect("'metric' key to have an object value")
                    .clone(),
            );
        }
    }

    metrics
}

fn get_unique_metrics(metrics: &[SerdeObject]) -> HashSet<String> {
    metrics
        .iter()
        .map(|metric| {
            metric
                .get("name")
                .expect("'name' key to have a string value")
                .to_string()
        })
        .collect::<HashSet<_>>()
}

fn count_metric_types(metrics: &[SerdeObject]) -> HashMap<String, usize> {
    let mut metric_types = HashMap::from([
        ("gauge".to_string(), 0),
        ("counter".to_string(), 0),
        ("histogram".to_string(), 0),
        ("distribution".to_string(), 0),
    ]);

    let metric_type_keys = metric_types.keys().cloned().collect::<Vec<_>>();

    'metric_iter: for metric in metrics {
        for metric_type in metric_type_keys.iter() {
            if metric.contains_key(metric_type) {
                *metric_types.entry(metric_type.to_string()).or_default() += 1;

                // Continue to the next metric if we found a type match
                continue 'metric_iter;
            }
        }

        println!(
            "Unknown metric: {}",
            metric
                .get("name")
                .expect("'name' key to have a string value")
        );
    }

    metric_types
}

fn write_metrics(filename: &str, metrics: &[String]) {
    std::fs::write(filename, metrics.join("\n")).expect(&format!("to write metrics to {filename}"));
}
