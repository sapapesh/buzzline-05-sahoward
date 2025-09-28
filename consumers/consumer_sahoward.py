"""
consumer_sahoward.py

A streaming consumer that:
- Reads ONE message at a time as it becomes available.
- Inserts that message into SQLite (persistent store).
- Computes average sentiment per author.
"""

import time
import pathlib
import sqlite3
import utils.utils_config as config
from utils.utils_logger import logger

# Import database helpers from sqlite_consumer_case
from .sqlite_consumer_case import init_db, insert_message


#####################################
# Function to calculate average sentiment
#####################################
def get_average_sentiment_by_author(db_path: pathlib.Path):
    """
    Calculate and log the average sentiment for each author in the database.
    """
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    author,
                    AVG(sentiment) AS avg_sentiment
                FROM
                    streamed_messages
                GROUP BY
                    author
                ORDER BY
                    avg_sentiment DESC;
                """
            )
            results = cursor.fetchall()

        logger.info("--- Average Sentiment by Author ---")
        if results:
            for author, avg in results:
                logger.info(f"Author: {author:<10} | Avg Sentiment: {avg:.2f}")
        else:
            logger.info("No messages found in the database.")
        logger.info("-----------------------------------")

    except Exception as e:
        logger.error(f"ERROR: Failed to compute average sentiment: {e}")


#####################################
# Example Message Source
#####################################
def fake_message_source():
    """
    Example generator that yields one fake message at a time.
    Replace this with your Kafka/RabbitMQ/etc. consumer.
    """
    import itertools, datetime
    authors = ["Alice", "Bob", "Charlie"]

    for i in itertools.count():
        yield {
            "message": f"Streaming message {i}",
            "author": authors[i % len(authors)],
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "category": "demo",
            "sentiment": round((i % 10) / 10, 2),  # rotating 0.0 → 0.9
            "keyword_mentioned": "demo",
            "message_length": len(f"Streaming message {i}"),
        }
        time.sleep(2)  # simulate arrival delay


#####################################
# Consumer Loop
#####################################
def consume_forever(db_path: pathlib.Path, message_source):
    """
    Continuously consume and insert one message at a time.
    """
    logger.info("Starting consumer loop...")
    for msg in message_source:
        try:
            logger.info(f"Received message: {msg}")
            insert_message(msg, db_path)
            get_average_sentiment_by_author(db_path)
        except Exception as e:
            logger.error(f"Error consuming message: {e}")
            time.sleep(1)  # backoff


#####################################
# Main
#####################################
def main():
    DATA_PATH: pathlib.Path = config.get_base_data_path()
    DB_PATH: pathlib.Path = DATA_PATH / "buzz.sqlite"

    # Initialize the database once
    init_db(DB_PATH)

    # Start consuming messages one by one
    consume_forever(DB_PATH, fake_message_source())


if __name__ == "__main__":
    main()
