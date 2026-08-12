import os
import json
import snowflake.connector
from openai import OpenAI
from dotenv import load_dotenv

# Resolve relative to this script's own location, not the current working
# directory - keeps this working whether run locally (ai/) or inside the
# Airflow container (/opt/airflow/ai/), where ../airflow/.env may not exist.
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "airflow", ".env")
if os.path.exists(_env_path):
    load_dotenv(_env_path)
else:
    load_dotenv()  # inside the container, real env vars are already set by docker-compose

MODEL = "llama-3.3-70b-versatile"   # Groq-hosted model; check console.groq.com/docs/models for current options
SAMPLE_N = int(os.getenv("SAMPLE_N", 5))
TOPICS = ["food quality", "delivery", "pricing", "service", "packaging", "other"]

client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1"
)

SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.
For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return null

Reply as JSON in this exact format:
{{
    "sentiment_label": "<sentiment_label>",
    "sentiment_score": <sentiment_score>,
    "topic": "<topic>",
    "key_issue": "<key_issue>"}}
"""


def _load_private_key():
    """Load the RSA private key and serialize to DER bytes, as the
    Snowflake connector expects for key-pair auth."""
    from cryptography.hazmat.primitives import serialization

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=_load_private_key(),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def create_output_table(cursor):
    cursor.execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ZOMATO.AI.REVIEW_ENRICHED (
            REVIEW_ID STRING,
            SENTIMENT_LABEL STRING,
            SENTIMENT_SCORE FLOAT,
            TOPIC STRING,
            KEY_ISSUE STRING,
            MODEL STRING,
            ENRICHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)


def get_reviews_to_enrich(cursor):
    cursor.execute(f"""
        SELECT REVIEW_ID, COMMENT
        FROM ZOMATO.RAW.REVIEWS
        WHERE REVIEW_ID NOT IN (SELECT REVIEW_ID FROM ZOMATO.AI.REVIEW_ENRICHED)
        LIMIT {SAMPLE_N}
    """)
    return cursor.fetchall()


def classify_review(comment):
    response = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": comment}
        ]
    )
    answer = response.choices[0].message.content
    return json.loads(answer)


def save_results(cursor, results):
    """Insert all the enriched rows into Snowflake in one go."""
    if not results:
        return
    print(f"Saving {len(results)} enriched reviews to Snowflake...")
    cursor.executemany(
        """
        INSERT INTO ZOMATO.AI.REVIEW_ENRICHED
            (review_id, sentiment_label, sentiment_score, topic, key_issue, model)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        results,
    )


def main():
    conn = get_connection()
    cursor = conn.cursor()

    create_output_table(cursor)

    reviews = get_reviews_to_enrich(cursor)
    if len(reviews) == 0:
        print("No new reviews to enrich.")
        cursor.close()
        conn.close()
        return

    print(f"Enriching {len(reviews)} reviews...")
    results = []
    for review_id, comment in reviews:
        print(f"Classifying review {review_id}: {comment}")
        try:
            labels = classify_review(comment)
            print(f"Labels for review {review_id}: {labels}")
            results.append((
                review_id,
                labels["sentiment_label"],
                labels["sentiment_score"],
                labels["topic"],
                labels["key_issue"],
                MODEL
            ))
        except Exception as e:
            print(f"Error occurred while classifying review {review_id}: {e}")

    save_results(cursor, results)
    print(f"Saved {len(results)} enriched reviews to Snowflake.")

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()