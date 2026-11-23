"""
embed-transcripts
Finds call transcripts not yet embedded and generates embeddings via Vertex AI.
Writes results to stage.call_transcript_embeddings in BigQuery.

Incremental by design: checks what's already embedded and skips it.
Safe to call multiple times — idempotent.
"""

import os
import functions_framework
from google.cloud import bigquery
from google import genai
from google.genai import types

GCP_PROJECT = os.environ.get("GCP_PROJECT", "")
LOCATION    = "us-central1"
EMBED_MODEL = "gemini-embedding-001"
EMBED_TABLE = f"{GCP_PROJECT}.stage.call_transcript_embeddings"
SOURCE_TABLE = f"{GCP_PROJECT}.autoelite_raw.call_transcripts"


@functions_framework.http
def task(request):

    bq     = bigquery.Client(project=GCP_PROJECT)
    client = genai.Client(vertexai=True, project=GCP_PROJECT, location=LOCATION)

    # ensure the embeddings table exists
    bq.query(f"""
        CREATE TABLE IF NOT EXISTS `{EMBED_TABLE}` (
            transcript_id STRING,
            transcript_text STRING,
            embedding ARRAY<FLOAT64>
        )
    """).result()

    # find transcripts not yet embedded
    rows = bq.query(f"""
        SELECT id AS transcript_id, body AS transcript_text
        FROM `{SOURCE_TABLE}`
        WHERE id NOT IN (
            SELECT transcript_id FROM `{EMBED_TABLE}`
        )
        ORDER BY id
    """).to_dataframe()

    if rows.empty:
        print("no new transcripts to embed")
        return {"embedded": 0}, 200

    print(f"embedding {len(rows)} transcripts")

    texts   = rows["transcript_text"].tolist()
    ids     = rows["transcript_id"].tolist()

    response = client.models.embed_content(
        model=EMBED_MODEL,
        contents=texts,
        config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
    )

    embed_rows = [
        {
            "transcript_id":   tid,
            "transcript_text": text,
            "embedding":       emb.values,
        }
        for tid, text, emb in zip(ids, texts, response.embeddings)
    ]

    errors = bq.insert_rows_json(EMBED_TABLE, embed_rows)
    if errors:
        print("BQ insert errors:", errors)
        return {"error": str(errors)}, 500

    print(f"inserted {len(embed_rows)} embeddings")
    return {"embedded": len(embed_rows)}, 200
