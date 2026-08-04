CREATE TABLE IF NOT EXISTS telemetry.gold_session_fatigue
(
    athlete_id          LowCardinality(String),
    window_start        DateTime64(3, 'UTC'),
    window_end          DateTime64(3, 'UTC'),
    device_session_ids  Array(UUID),
    set_count           UInt16,
    total_reps          UInt32,
    volume_load_kg      Float64,
    avg_rpe             Float32,
    rpe_drift           Float32,
    rep_drop_pct        Float32,
    fatigue_index       Float32,
    closed_by           LowCardinality(String),
    emitted_at          DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(emitted_at)
ORDER BY (athlete_id, window_start);
