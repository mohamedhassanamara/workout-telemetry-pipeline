CREATE OR REPLACE VIEW telemetry.gold_readiness AS
SELECT
    r.athlete_id                    AS athlete_id,
    r.recovery_date                 AS day,
    r.sleep_hours                   AS sleep_hours,
    r.sleep_quality                 AS sleep_quality,
    r.resting_hr_bpm                AS resting_hr_bpm,
    round(r.rhr_baseline, 1)        AS rhr_baseline_28d,
    a.acwr                          AS acwr_yesterday,
    greatest(0, least(100, round(
          40 * least(r.sleep_hours / 8, 1)
        + 20 * (r.sleep_quality - 1) / 4
        + 25 * greatest(0, 1 - greatest(0, r.resting_hr_bpm - r.rhr_baseline) / 8)
        + 15 * if(a.acwr IS NULL OR a.acwr <= 1.3, 1, greatest(0, 1 - (a.acwr - 1.3) / 0.5))
    )))                             AS readiness_score,
    multiIf(readiness_score >= 75, 'train_hard',
            readiness_score >= 55, 'train_normal',
                                   'deload_or_rest') AS recommendation
FROM
(
    SELECT
        athlete_id, recovery_date, sleep_hours, sleep_quality, resting_hr_bpm,
        recovery_date - 1 AS prev_day,
        if(isFinite(baseline_raw), baseline_raw, toFloat64(resting_hr_bpm)) AS rhr_baseline
    FROM
    (
        SELECT
            athlete_id, recovery_date, sleep_hours, sleep_quality, resting_hr_bpm,
            avg(resting_hr_bpm) OVER (
                PARTITION BY athlete_id ORDER BY recovery_date
                ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
            ) AS baseline_raw
        FROM telemetry.silver_daily_recovery FINAL
    )
) AS r
LEFT JOIN telemetry.gold_acwr AS a
    ON a.athlete_id = r.athlete_id AND a.day = r.prev_day;
