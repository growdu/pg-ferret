use aya::maps::{PerfEventArray, MapData};
use aya::util::online_cpus;
use bpf::{attach_uprobes, init_bpf};
use log::info;
use metrics::init_metrics;
use receive::listen_to_cpu;
use std::env;
use std::error::Error;
use tokio::signal;
use tracing::TraceEmitter;

mod bpf;
mod generated;
mod metrics;
mod receive;
mod tracing;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    info!("Starting pg-ferret trace test");
    let endpoint = Some(String::from("http://192.168.3.99:4317"));
    let tracing = TraceEmitter::initialise(endpoint)?;
   

    // Initialise the metrics prometheus exporter. We'll collect metrics
    // about the postgres queries and this userspace collector, and expose
    // them on a HTTP /metrics endpoint in prometheus format.
    init_metrics();
    tracing.start_span("root", 1, None, true, true, true, None, vec![]);
    tracing.end_span("root", 1);

    Ok(())
}