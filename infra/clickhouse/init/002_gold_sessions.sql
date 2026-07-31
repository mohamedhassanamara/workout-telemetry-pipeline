CREATE OR REPLACE VIEW telemetry.gold_session_volume AS
SELECT
    athlete_id,
    session_id,
    toDate(min(event_time))                                AS session_date,
    min(event_time)                                        AS started_at,
    max(event_time)                                        AS ended_at,
    dateDiff('minute', min(event_time), max(event_time))   AS duration_min,
    count()                                                AS total_sets,
    sum(reps)                                              AS total_reps,
    round(sum(reps * weight_kg), 1)                        AS volume_load_kg,
    round(avg(rpe), 2)                                     AS avg_rpe,
    round(avg(rpe) * greatest(dateDiff('minute', min(event_time), max(event_time)), 1), 1) AS srpe_load
FROM telemetry.silver_workout_sets FINAL
GROUP BY athlete_id, session_id;

CREATE OR REPLACE VIEW telemetry.gold_exercise_volume AS
SELECT
    athlete_id,
    session_id,
    exercise,
    toDate(min(event_time))           AS session_date,
    count()                           AS set_count,
    sum(reps)                         AS total_reps,
    max(weight_kg)                    AS top_weight_kg,
    round(sum(reps * weight_kg), 1)   AS volume_load_kg,
    round(avg(rpe), 2)                AS avg_rpe
FROM telemetry.silver_workout_sets FINAL
GROUP BY athlete_id, session_id, exercise;
