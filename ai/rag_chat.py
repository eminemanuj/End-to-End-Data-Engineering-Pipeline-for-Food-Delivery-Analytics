import os
import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
import cohere
from openai import OpenAI
from dotenv import load_dotenv

# Resolve relative to this script's own location, not the current working
# directory - reads from ai/.env (local testing) rather than airflow/.env
# (container config), since those need different path formats.
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    load_dotenv(_env_path)
else:
    load_dotenv()

EMBEDDING_MODEL = "embed-english-v3.0"   # Cohere embedding model
CHAT_MODEL = "llama-3.3-70b-versatile"   # Groq-hosted chat model
NEW_REVIEWS = 500
TOP_K = 5
CACHE_FILE = "review_embeddings.parquet"

cohere_client = cohere.ClientV2(api_key=os.getenv("COHERE_API_KEY"))

groq_client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1"
)


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


def read_reviews_from_snowflake():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=_load_private_key(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    conn.cursor().execute(f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}")

    query = f"""
        SELECT REVIEW_ID, CITY, RATING, COMMENT
        FROM ZOMATO.STAGING.STG_REVIEWS
        SAMPLE ({NEW_REVIEWS} ROWS)
    """
    df = conn.cursor().execute(query).fetch_pandas_all()
    conn.close()

    df.columns = [col.lower() for col in df.columns]
    return df


def embed(texts, input_type="search_document"):
    """Cohere requires an input_type: 'search_document' when embedding the
    review corpus, 'search_query' when embedding the user's question - this
    asymmetry actually improves retrieval quality vs. a single shared type.

    Cohere also caps each request at 96 texts, so larger lists are chunked."""
    BATCH_SIZE = 96
    all_embeddings = []

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i + BATCH_SIZE]
        response = cohere_client.embed(
            texts=batch,
            model=EMBEDDING_MODEL,
            input_type=input_type,
            embedding_types=["float"],
        )
        all_embeddings.extend(response.embeddings.float_)

    return all_embeddings


@st.cache_data()
def load_reviews():
    if os.path.exists(CACHE_FILE):
        return pd.read_parquet(CACHE_FILE)

    df = read_reviews_from_snowflake()
    df['embedding'] = embed(df['comment'].tolist(), input_type="search_document")
    df.to_parquet(CACHE_FILE)
    return df


st.title("Chat with your Zomato Reviews")
st.caption(f"Searching {NEW_REVIEWS} reviews, answering with {CHAT_MODEL} model")


def cosine_similarity(vec_a, vec_b):
    return np.dot(vec_a, vec_b) / (np.linalg.norm(vec_a) * np.linalg.norm(vec_b))


def find_similar_reviews(question, df):
    question_vector = embed([question], input_type="search_query")[0]

    scores = []
    for review_vector in df['embedding']:
        scores.append(cosine_similarity(question_vector, review_vector))

    df = df.copy()
    df['score'] = scores
    return df.nlargest(TOP_K, 'score')


def ask_llm(question, top_reviews):
    context = ""

    for _, row in top_reviews.iterrows():
        context += f" ({row['city']}, {row['rating']} stars) {row['comment']}\n"

    system_prompt = (
        "Answer ONLY using the customer reviews provided. "
        "Be concise. If the reviews don't cover it, say so."
    )

    user_prompt = f"Question: {question}\n\nReviews:\n{context}"

    response = groq_client.chat.completions.create(
        model=CHAT_MODEL,
        temperature=0.2,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    )
    return response.choices[0].message.content


review_df = load_reviews()

question = st.text_input(
    "Ask a question about your reviews:",
    placeholder="e.g. What are the most common complaints about delivery?"
)

if question:
    top_reviews = find_similar_reviews(question, review_df)
    answer = ask_llm(question, top_reviews)

    st.markdown(f"**Answer:**")
    st.write(answer)

    with st.expander("Reviews used to build this answer"):
        st.dataframe(top_reviews[['city', 'rating', 'comment']], hide_index=True)