"""
consumer_sahoward.py

Consumes messages one at a time and stores them in SQLite.
Maintains a rolling average sentiment per author in an
`author_sentiment` table, creating the table on-the-fly if missing.
"""

#####################################
# Import Modules
#####################################
import pathlib
import sqlite3
import time
import utils.utils_config as config
from utils.utils_logger import logger

# Relative import to DB helper in the same folder
from .sqlite_consumer_case import init_db  # DB helper

#####################################
# Path Setup
#####################################
DATA_PATH: pathlib.Path = config.get_base_data_path()
DB_PATH: pathlib.Path = DATA_PATH / "buzz.sqlite"

# Initialize DB (creates streamed_messages and author_sentiment tables if missing)
init_db(DB_PATH)


#####################################
# Ensure author_sentiment exists
#####################################
def ensure_author_sentiment_table():
    """Create author_sentiment table if it does not exist."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS author_sentiment (
                    author TEXT PRIMARY KEY,
                    avg_sentiment REAL,
                    message_count INTEGER
                )
                """
            )
            conn.commit()
    except Exception as e:
        logger.error(f"ERROR: Failed to ensure author_sentiment table: {e}")


#####################################
# Insert message and update author sentiment
#####################################
def insert_message_and_update(message: dict) -> None:
    """
    Insert a single message into streamed_messages and update the
    rolling average sentiment in author_sentiment.
    """
    ensure_author_sentiment_table()  # <-- self-healing

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # Insert into streamed_messages
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message["message"],
                    message["author"],
                    message["timestamp"],
                    message["category"],
                    message["sentiment"],
                    message["keyword_mentioned"],
                    message["message_length"],
                ),
            )

            # Update author_sentiment table
            cursor.execute(
                "SELECT avg_sentiment, message_count FROM author_sentiment WHERE author = ?",
                (message["author"],),
            )
            row = cursor.fetchone()

            if row:
                old_avg, count = row
                new_count = count + 1
                new_avg = (old_avg * count + message["sentiment"]) / new_count
                cursor.execute(
                    "UPDATE author_sentiment SET avg_sentiment = ?, message_count = ? WHERE author = ?",
                    (new_avg, new_count, message["author"]),
                )
            else:
                cursor.execute(
                    "INSERT INTO author_sentiment (author, avg_sentiment, message_count) VALUES (?, ?, ?)",
                    (message["author"], message["sentiment"], 1),
                )

            conn.commit()
        logger.info(f"Inserted message and updated sentiment for {message['author']}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert/update message: {e}")


#####################################
# Display author sentiment
#####################################
def display_author_sentiment():
    """Query author_sentiment and log current averages."""
    ensure_author_sentiment_table()  # <-- self-healing

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT author, avg_sentiment, message_count FROM author_sentiment ORDER BY avg_sentiment DESC"
            )
            rows = cursor.fetchall()

        logger.info("--- Author Sentiment Summary ---")
        for author, avg, count in rows:
            logger.info(f"Author: {author:<10} | Avg Sentiment: {avg:.2f} | Messages: {count}")
        logger.info("--------------------------------")
    except Exception as e:
        logger.error(f"ERROR: Failed to display author sentiment: {e}")


#####################################
# Placeholder for message source
#####################################
def get_next_message():
    """
    Placeholder generator function.
    Replace this with your actual streaming source (Kafka, RabbitMQ, etc.).
    """
    import itertools, datetime
    authors = ["Alice", "Bob", "Charlie"]
    for i in itertools.count():
        yield {
            "message": f"Streaming message {i}",
            "author": authors[i % len(authors)],
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "category": "demo",
            "sentiment": round((i % 10) / 10, 2),  # 0.0 → 0.9
            "keyword_mentioned": "demo",
            "message_length": len(f"Streaming message {i}"),
        }
        time.sleep(2)


#####################################
# Consumer Loop
#####################################
def consume_messages():
    """Continuously consume and process one message at a time."""
    logger.info("Starting consumer loop.")
    message_source = get_next_message()
    for message in message_source:
        insert_message_and_update(message)
        display_author_sentiment()


#####################################
# Main
#####################################
if __name__ == "__main__":
    # IMPORTANT: run from project root with:
    # python -m consumers.consumer_sahoward
    consume_messages()
