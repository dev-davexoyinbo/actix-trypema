//! An in-memory `metrics` recorder for asserting counter values.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

/// Counters keyed as `name{label=value,...}`; gauges and histograms are ignored.
#[derive(Default)]
pub struct CountingRecorder {
    counters: Mutex<HashMap<String, Arc<CounterCell>>>,
}

#[derive(Default)]
struct CounterCell(AtomicU64);

impl CounterFn for CounterCell {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::SeqCst);
    }

    fn absolute(&self, value: u64) {
        self.0.fetch_max(value, Ordering::SeqCst);
    }
} // end impl

impl CountingRecorder {
    pub fn value(&self, key: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(key)
            .map_or(0, |cell| cell.0.load(Ordering::SeqCst))
    }
} // end impl

impl Recorder for CountingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let labels: Vec<String> = key
            .labels()
            .map(|label| format!("{}={}", label.key(), label.value()))
            .collect();
        let name = format!("{}{{{}}}", key.name(), labels.join(","));
        let cell = Arc::clone(self.counters.lock().unwrap().entry(name).or_default());
        Counter::from_arc(cell)
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
} // end impl
