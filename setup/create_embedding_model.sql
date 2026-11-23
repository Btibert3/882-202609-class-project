-- One-time setup: create a BigQuery remote model pointing to Vertex AI embeddings.
-- Run this once before triggering pipeline_embed_transcripts.
--
-- Prerequisites:
--   1. Create a BigQuery external connection (type: Vertex AI) in the BQ console
--      under "External connections" and note the connection ID.
--   2. Grant the connection's service account the Vertex AI User role in IAM.
--   3. Replace YOUR_REGION.YOUR_CONNECTION_ID below with your actual connection.
--
-- After running this, trigger pipeline_embed_transcripts in Airflow.

CREATE OR REPLACE MODEL `YOUR_PROJECT.stage.embed_model`
  REMOTE WITH CONNECTION `us-central1.YOUR_CONNECTION_ID`
  OPTIONS (endpoint = 'text-embedding-005');
