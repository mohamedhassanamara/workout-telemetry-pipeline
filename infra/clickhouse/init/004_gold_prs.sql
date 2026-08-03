CREATE OR REPLACE VIEW telemetry.gold_personal_records AS
SELECT athlete_id, exercise, event_time, session_id, weight_kg, reps, e1rm, previous_best,
       round(e1rm - previous_best, 1) AS improvement_kg
FROM
(
    SELECT
        athlete_id, exercise, event_time, session_id, weight_kg, reps,
        round(weight_kg * (1 + reps / 30), 1) AS e1rm,
        max(round(weight_kg * (1 + reps / 30), 1)) OVER (
            PARTITION BY athlete_id, exercise
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS previous_best
    FROM telemetry.silver_workout_sets FINAL
    WHERE reps > 0 AND weight_kg > 0
)
WHERE previous_best > 0 AND e1rm > previous_best;

CREATE OR REPLACE VIEW telemetry.gold_best_e1rm AS
SELECT
    athlete_id,
    exercise,
    max(round(weight_kg * (1 + reps / 30), 1))       AS best_e1rm_kg,
    argMax(event_time, weight_kg * (1 + reps / 30))  AS achieved_at
FROM telemetry.silver_workout_sets FINAL
WHERE reps > 0 AND weight_kg > 0
GROUP BY athlete_id, exercise;
