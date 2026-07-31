CREATE OR REPLACE VIEW telemetry.gold_daily_load AS
WITH
    bounds AS (
        SELECT athlete_id, min(session_date) AS first_day, max(session_date) AS last_day
        FROM telemetry.gold_session_volume
        GROUP BY athlete_id
    ),
    calendar AS (
        SELECT
            athlete_id,
            arrayJoin(arrayMap(i -> first_day + i,
                               range(toUInt32(dateDiff('day', first_day, last_day) + 1)))) AS day
        FROM bounds
    ),
    daily AS (
        SELECT athlete_id, session_date, sum(srpe_load) AS day_srpe, sum(volume_load_kg) AS day_volume
        FROM telemetry.gold_session_volume
        GROUP BY athlete_id, session_date
    )
SELECT
    c.athlete_id                AS athlete_id,
    c.day                       AS day,
    coalesce(d.day_srpe, 0)     AS srpe_load,
    coalesce(d.day_volume, 0)   AS volume_load_kg
FROM calendar AS c
LEFT JOIN daily AS d ON c.athlete_id = d.athlete_id AND c.day = d.session_date;

CREATE OR REPLACE VIEW telemetry.gold_acwr AS
SELECT
    athlete_id,
    day,
    srpe_load,
    round(acute_raw, 1)   AS acute_7d,
    round(chronic_raw, 1) AS chronic_28d,
    acwr,
    multiIf(acwr IS NULL, 'insufficient_history',
            acwr < 0.8,   'undertraining',
            acwr <= 1.3,  'sweet_spot',
            acwr <= 1.5,  'caution',
                          'high_risk') AS risk_zone
FROM
(
    SELECT
        athlete_id, day, srpe_load, acute_raw, chronic_raw,
        if(days_in_window >= 28 AND chronic_raw > 0, round(acute_raw / chronic_raw, 2), NULL) AS acwr
    FROM
    (
        SELECT
            athlete_id,
            day,
            srpe_load,
            avg(srpe_load) OVER w7  AS acute_raw,
            avg(srpe_load) OVER w28 AS chronic_raw,
            count()        OVER w28 AS days_in_window
        FROM telemetry.gold_daily_load
        WINDOW
            w7  AS (PARTITION BY athlete_id ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
            w28 AS (PARTITION BY athlete_id ORDER BY day ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)
    )
);
