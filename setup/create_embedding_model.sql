-- One-time setup: create a BigQuery remote model pointing to Vertex AI embeddings.
-- Run this once before triggering pipeline_embed_transcripts.
--
-- Prerequisites:
--   1. Create a BigQuery external connection in the BQ console under "External connections"
--      Type: "Agent Platform remote models, remote functions, Lakehouse and Spanner (Cloud Resource)"
--      Region: US (multiple regions) — must match the region of your stage dataset
--   2. Grant the connection's service account the Vertex AI User role in IAM.
--   3. Replace YOUR_PROJECT and YOUR_CONNECTION_ID below with your actual values.
--
-- After running this, trigger pipeline_embed_transcripts in Airflow.

CREATE OR REPLACE MODEL `YOUR_PROJECT.stage.embed_model`
  REMOTE WITH CONNECTION `us.YOUR_CONNECTION_ID`
  OPTIONS (endpoint = 'text-embedding-005');
