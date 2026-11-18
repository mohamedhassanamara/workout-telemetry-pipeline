flowchart LR
  SIM[Python simulator<br/>sets, recovery, chaos] -->|key = athlete_id| RP[(Redpanda<br/>sets / recovery topics)]
  RP --> BR[Bronze sink]
  RP --> SL[Silver loader]
  RP --> FW[Fatigue windows]
  BR --> S3[(LocalStack S3<br/>raw gzip JSONL)]
  SL --> CH[(ClickHouse<br/>silver + gold)]
  FW --> CH
  SL -->|invalid| DLQ[(workout.dlq.v1)]
  FW -->|late| DLQ
  DLQ -->|replay| RP
  DLQ --> BR
