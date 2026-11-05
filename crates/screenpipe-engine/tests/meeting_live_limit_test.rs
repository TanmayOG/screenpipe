// screenpipe — AI that knows everything you've seen, said, or heard
// https://screenpipe.com

//! A long call must not be cut off by the live-session limit (issue #7879).
//!
//! Runs the real `start_meeting_streaming_loop` on a fake clock. The provider
//! endpoint is a closed local port, so nothing leaves the machine: the limit
//! only reads time, the live flag, and the meeting detector.

use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use futures::{FutureExt, StreamExt};
use screenpipe_audio::{
    core::device::DeviceType,
    meeting_detector::{ActiveMeeting, MeetingDetector},
    meeting_streaming::{
        start_meeting_streaming_loop, MeetingAudioFrame, MeetingAudioTap, MeetingStreamingConfig,
    },
};
use screenpipe_db::DatabaseManager;
use serde_json::{json, Value};
use serial_test::serial;
use tokio::sync::{broadcast, RwLock};

const HOUR: Duration = Duration::from_secs(3600);
const MINUTE: Duration = Duration::from_secs(60);
const STEP: Duration = Duration::from_secs(10);
/// The loop checks every 30s and this test samples every `STEP`.
const CHECK_SLACK: Duration = Duration::from_secs(60);
const TIME_LIMIT: &str = "live meeting maximum duration reached";
const INACTIVE: &str = "live audio inactive";

#[derive(Clone, Copy)]
enum Sensor {
    NoDetector,
    SeesCall,
    ManualMeeting,
}

struct Call {
    meeting_id: i64,
    sensor: Sensor,
    sensor_lost_after: Option<Duration>,
    audio_stops_after: Option<Duration>,
    run_for: Duration,
}

impl Call {
    fn new(meeting_id: i64, sensor: Sensor, run_for: Duration) -> Self {
        Self {
            meeting_id,
            sensor,
            sensor_lost_after: None,
            audio_stops_after: None,
            run_for,
        }
    }
}

#[derive(Debug)]
struct Outcome {
    auto_end: Option<(Duration, String)>,
    sessions_ended: u32,
}

impl Outcome {
    fn assert_auto_ended(&self, expected_after: Duration, expected_reason: &str) {
        let (after, reason) = self.auto_end.as_ref().expect("the meeting should auto-end");
        assert_eq!(reason, expected_reason);
        assert!(
            *after >= expected_after && *after <= expected_after + CHECK_SLACK,
            "auto-ended after {after:?}, expected about {expected_after:?}"
        );
    }
}

fn voiced_frame() -> MeetingAudioFrame {
    MeetingAudioFrame {
        samples: Arc::new(vec![0.1; 480]),
        device_name: "Speakers (Realtek)".to_string(),
        device_type: DeviceType::Output,
        sample_rate: 16_000,
        channels: 1,
        captured_at_unix_ms: 0,
    }
}

fn detector_for(sensor: Sensor) -> Option<Arc<MeetingDetector>> {
    let manual = match sensor {
        Sensor::NoDetector => return None,
        Sensor::SeesCall => false,
        Sensor::ManualMeeting => true,
    };
    let detector = Arc::new(MeetingDetector::new());
    detector.set_v2_in_meeting(true);
    detector.set_active_meeting(Some(ActiveMeeting {
        pid: None,
        bundle_id: None,
        manual,
    }));
    Some(detector)
}

async fn run(call: Call) -> Outcome {
    std::env::set_var("SCREENPIPE_MEETING_REALTIME_URL", "ws://127.0.0.1:9");
    let db = Arc::new(
        DatabaseManager::new("sqlite::memory:", Default::default())
            .await
            .unwrap(),
    );
    let (tx, rx) = broadcast::channel(1024);
    let tap = MeetingAudioTap::new(tx.clone(), Arc::new(AtomicBool::new(false)));
    let config = MeetingStreamingConfig::from_settings(
        true,
        "screenpipe-cloud",
        Some("test-token".to_string()),
        None,
        None,
        None,
    );
    let detector = detector_for(call.sensor);

    let mut auto_end_sub =
        screenpipe_events::subscribe_to_event::<Value>("meeting_auto_end_requested");
    let mut session_ended_sub =
        screenpipe_events::subscribe_to_event::<Value>("meeting_streaming_session_ended");

    let handle = start_meeting_streaming_loop(
        config,
        tap,
        rx,
        db,
        Arc::new(RwLock::new(None)),
        None,
        Arc::new(RwLock::new(detector.clone())),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    screenpipe_events::send_event(
        "meeting_started",
        json!({
            "meeting_id": call.meeting_id,
            "app": "Microsoft Teams",
            "detection_source": "audio_process",
        }),
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let mut outcome = Outcome {
        auto_end: None,
        sessions_ended: 0,
    };
    while started.elapsed() < call.run_for && outcome.auto_end.is_none() {
        let elapsed = started.elapsed();
        if call.audio_stops_after.is_none_or(|at| elapsed < at) {
            let _ = tx.send(voiced_frame());
        }
        if call.sensor_lost_after.is_some_and(|at| elapsed >= at) {
            if let Some(detector) = &detector {
                detector.set_v2_in_meeting(false);
                detector.set_active_meeting(None);
            }
        }

        tokio::time::advance(STEP).await;
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }

        while let Some(event) = auto_end_sub.next().now_or_never().flatten() {
            if event.data["meeting_id"].as_i64() == Some(call.meeting_id) {
                let reason = event.data["reason"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                outcome.auto_end = Some((started.elapsed(), reason));
            }
        }
        while let Some(event) = session_ended_sub.next().now_or_never().flatten() {
            if event.data["meeting_id"].as_i64() == Some(call.meeting_id) {
                outcome.sessions_ended += 1;
            }
        }
    }

    tokio::time::resume();
    handle.abort();
    outcome
}

#[tokio::test]
#[serial]
async fn detected_call_keeps_streaming_past_the_limit() {
    let outcome = run(Call::new(7001, Sensor::SeesCall, 5 * HOUR)).await;

    assert!(outcome.auto_end.is_none(), "{outcome:?}");
    assert_eq!(
        outcome.sessions_ended, 0,
        "live transcription must carry on without a restart"
    );
}

#[tokio::test]
#[serial]
async fn hard_ceiling_ends_a_detected_call() {
    let outcome = run(Call::new(7002, Sensor::SeesCall, 13 * HOUR)).await;

    outcome.assert_auto_ended(12 * HOUR, TIME_LIMIT);
}

#[tokio::test]
#[serial]
async fn manual_meeting_still_ends_at_the_limit() {
    let outcome = run(Call::new(7003, Sensor::ManualMeeting, 3 * HOUR)).await;

    outcome.assert_auto_ended(2 * HOUR, TIME_LIMIT);
}

#[tokio::test]
#[serial]
async fn meeting_without_a_detector_still_ends_at_the_limit() {
    let outcome = run(Call::new(7004, Sensor::NoDetector, 3 * HOUR)).await;

    outcome.assert_auto_ended(2 * HOUR, TIME_LIMIT);
}

#[tokio::test]
#[serial]
async fn call_the_detector_lost_ends_at_the_next_check() {
    let outcome = run(Call {
        sensor_lost_after: Some(3 * HOUR),
        ..Call::new(7005, Sensor::SeesCall, 5 * HOUR)
    })
    .await;

    outcome.assert_auto_ended(4 * HOUR, TIME_LIMIT);
}

#[tokio::test]
#[serial]
async fn silence_ends_a_detected_call_after_an_extension() {
    let audio_stops_after = 2 * HOUR + 30 * MINUTE;
    let outcome = run(Call {
        audio_stops_after: Some(audio_stops_after),
        ..Call::new(7006, Sensor::SeesCall, 4 * HOUR)
    })
    .await;

    outcome.assert_auto_ended(audio_stops_after + 15 * MINUTE, INACTIVE);
}
