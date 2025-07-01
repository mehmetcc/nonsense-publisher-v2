use crate::nonsense::Nonsense;
use crossbeam_channel::Sender;
use std::time::Duration;
use tracing::{debug, debug_span, error, error_span, info, info_span, instrument};

pub fn start_generators(num_threads: usize, tx: Sender<Nonsense>, throttle: Option<Duration>) {
    for i in 0..num_threads {
        let tx_clone = tx.clone();
        let throttle_clone = throttle.clone();
        rayon::spawn(move || generate_loop(i, tx_clone, throttle_clone));
    }
}

#[instrument]
fn generate_loop(thread_id: usize, tx: Sender<Nonsense>, throttle: Option<Duration>) {
    info_span!("generator_started", thread_id = thread_id)
        .in_scope(|| info!("Starting thread with id: {:?}", thread_id));
    loop {
        let nonsense = Nonsense::new();
        if tx.send(nonsense).is_err() {
            error_span!("send_failed").in_scope(|| {
                error!("Failed to send nonsense to channel");
            });
            break;
        }

        if let Some(duration) = throttle {
            info_span!("throttle_started", duration = format!("{:?}", duration)).in_scope(|| {
                info!("Throttling");
            });
            std::thread::sleep(duration);
        }
    }
}
