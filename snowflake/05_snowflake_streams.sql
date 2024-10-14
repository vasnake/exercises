-- Introduction to Streams https://youtu.be/EQ44K5GfgDw?t=2198
/*
Stream: a way to implement Change Data Capture
How it is done: DML ops listen, react
Typical data flow: s3 files -> (raw)staging schema -> prod schema
Scenario: ingest new files to staging, catch cdc stream on staging data, apply stream to prod
Use cases for streaming: update search history, stream analytics, anomaly detection, audit transaction log, event-driven platform, ETL for incremental pipelines
*/

-- Implementing Standard Streams https://youtu.be/EQ44K5GfgDw?t=2330
