# End-to-End Data Engineering Pipeline for Food Delivery Analytics

A batch ELT pipeline for a Zomato-style food delivery dataset: raw CSVs land in S3, get loaded into Snowflake, modeled through a medallion architecture with dbt, orchestrated with Airflow, and enriched with an AI layer covering review sentiment analysis, retrieval-augmented chat, and natural-language-to-SQL querying.

## Architecture

```
S3 (raw CSVs) → Snowflake RAW → dbt STAGING → dbt MARTS → Airflow (orchestration)
                                                    ↓
                                    AI layer: LLM enrichment · RAG chat · text-to-SQL
```

| Layer | Tool | What it does |
|---|---|---|
| Lake | Amazon S3 | Raw CSVs (~10M orders, 23M order items, 300K reviews) |
| Bronze | Snowflake RAW | Loaded via `COPY INTO` from an external stage |
| Silver | Snowflake STAGING (dbt) | Cleaned, typed, conformed staging views |
| Gold | Snowflake MARTS (dbt) | Dimensions, incremental fact tables, business marts |
| AI | Snowflake AI schema | LLM-enriched review sentiment/topic table |
| Orchestration | Apache Airflow (Docker) | Daily DAG: load → dbt build → enrich → dbt build (AI marts) |
| Apps | Streamlit | RAG chat over reviews, natural-language SQL querying |

## Tech stack

- **Ingestion:** AWS S3, Snowflake external stage
- **Transformation:** dbt (staging models, dimensional models, incremental fact tables, data tests)
- **Orchestration:** Apache Airflow (Docker Compose, LocalExecutor)
- **AI:** Groq (Llama 3.3 70B) for LLM enrichment, RAG generation, and text-to-SQL; Cohere for review embeddings
- **Serving:** Streamlit
- **Auth:** Snowflake key-pair authentication (RSA)

## What's in the AI layer

1. **LLM review enrichment** — classifies free-text reviews into sentiment label/score, topic, and key issue, written back to a Snowflake table (`ZOMATO.AI.REVIEW_ENRICHED`) that feeds a downstream dbt mart.
2. **RAG chat over reviews** — embeds a sample of reviews with Cohere, retrieves the most relevant reviews by cosine similarity for a user's question, and generates a grounded answer with Groq.
3. **Text-to-SQL** — turns a natural-language question into a single `SELECT` query against the marts layer, with a keyword-based safety guard before execution.

## Project structure

```
snowflake/        Snowflake setup SQL (warehouse, schemas, stage, tables, grants)
zomato/           dbt project (staging + marts models, tests, macros)
airflow/          Airflow DAG, Docker Compose setup
ai/               LLM enrichment, RAG chat, and text-to-SQL Streamlit apps
```

## Setup

1. Create an S3 bucket and upload the source CSVs under `raw/<table>/`.
2. Create a Snowflake account, then run `snowflake/setup.sql` in a Snowsight worksheet (fill in your bucket name, IAM credentials, and RSA public key first — it's a template, not meant to run as-is).
3. Set up key-pair authentication for your Snowflake user (see [Snowflake's key-pair auth docs](https://docs.snowflake.com/en/user-guide/key-pair-auth)) — `setup.sql` includes the `ALTER USER ... SET RSA_PUBLIC_KEY` step.
4. Copy `airflow/example.env` to `airflow/.env` and fill in your Snowflake account, username, private key path, and Groq API key.
5. `cd airflow && docker-compose up -d` — Airflow UI at `http://localhost:8080`.
6. For the Streamlit apps, create `ai/.env` with the same Snowflake credentials plus `GROQ_API_KEY` and `COHERE_API_KEY`, then `streamlit run ai/rag_chat.py` or `streamlit run ai/text_to_sql.py`.

## Notes

- Fact tables (`fct_orders`, `fact_order_items`) are built as dbt **incremental models** to avoid full rebuilds on a 10M+ row table.
- The AI enrichment step writes to Snowflake idempotently (only unenriched reviews are processed on each run), so it's safe to run repeatedly via Airflow's daily schedule.
- Snowflake authentication uses key-pair auth throughout rather than passwords, in line with Snowflake's deprecation of password-only sign-in.

## Challenges along the way

A few real issues hit and resolved during the build, worth noting since they're common in production data engineering:

- **IAM trust policy debugging** — an early attempt at a keyless S3↔Snowflake storage integration (via `AssumeRole`) kept failing despite a byte-verified-correct trust policy; switched to direct IAM user credentials on the stage instead, which is simpler to reason about for a single-account setup.
- **dbt schema misconfiguration** — mart models were silently landing in the `STAGING` schema instead of `MARTS` due to a missing `+schema:` config in `dbt_project.yml`. Fixed and verified against Snowflake's `SHOW TABLES`/`SHOW VIEWS`.
- **Account lockout recovery** — an overly broad Snowflake network policy locked out all access (UI included) after a dynamic IP changed. Recovered by provisioning a fresh account and switching to key-pair authentication, which avoids the network-policy dependency entirely.
- **Cross-environment config** — running the same Python scripts both locally and inside a Docker container required separating local (`ai/.env`) and container (`airflow/.env`) configs, since file paths and environment variable resolution differ between the two.