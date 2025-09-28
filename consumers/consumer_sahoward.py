""" kafka_sqlite_consumer.py 

This consumer reads messages from a Kafka topic one at a time and 
processes them by:
1. Storing the raw message in 'streamed_messages'.
2. Recalculating and updating the author's average sentiment in 
   'author_sentiment_summary'.

It runs continuously until manually stopped (Ctrl+C).

"""

#####################################
# Import Modules
#####################################

# stdlib
import json
import pathlib 
import sqlite3
import time

# external
from kafka import KafkaConsumer # kafka-python-ng

# local
import utils.utils_config as config
from utils.utils_logger import logger

# Import the necessary database functions from the previous consumer file
# NOTE: Assuming the functions from consumer_sahoward.py are available/imported
# For this example, we redefine the core functions here for completeness.

#####################################
# Database Functions (from consumer_sahoward.py)
#####################################

# NOTE: The init_db, update_author_average_sentiment, and 
# insert_message functions from the previous response are necessary 
# for this consumer to work. They are defined below.

def init_db(db_path: pathlib.Path):
    """Initializes the two required tables in the SQLite database."""
    logger.info("Calling SQLite init_db() to set up tables.")
    try:
        # Create directories if needed (omitted for brevity, assume utils.utils_config handles it)
        # os.makedirs(os.path.dirname(db_path), exist_ok=True) 
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Raw messages table
            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT, author TEXT, timestamp TEXT, category TEXT, 
                    sentiment REAL, keyword_mentioned TEXT, message_length INTEGER
                )
            """
            )

            # Processed results table (Average Sentiment)
            cursor.execute("DROP TABLE IF EXISTS author_sentiment_summary;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS author_sentiment_summary (
                    author TEXT PRIMARY KEY,
                    average_sentiment REAL,
                    total_messages INTEGER
                )
            """
            )
            conn.commit()
        logger.info(f"SUCCESS: Database initialized at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize SQLite database: {e}")


def update_author_average_sentiment(author: str, db_path: pathlib.Path) -> None:
    """Calculates the current average sentiment for an author and updates the summary table."""
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            
            # 1. Calculate the new average sentiment and count
            cursor.execute(
                """
                SELECT AVG(sentiment), COUNT(*)
                FROM streamed_messages 
                WHERE author = ?
                """,
                (author,)
            )
            
            avg_sentiment, total_messages = cursor.fetchone()

            # 2. Upsert (UPDATE or INSERT) the processed result
            cursor.execute(
                """
                INSERT INTO author_sentiment_summary (author, average_sentiment, total_messages)
                VALUES (?, ?, ?)
                ON CONFLICT(author) DO UPDATE SET
                    average_sentiment = excluded.average_sentiment,
                    total_messages = excluded.total_messages;
                """,
                (author, avg_sentiment, total_messages)
            )
            conn.commit()
            logger.info(f"    -> [Processed Result Stored]: Updated {author}'s Avg Sentiment to {avg_sentiment:.2f} (Count: {total_messages})")
            
    except Exception as e:
        logger.error(f"ERROR: Failed to update average sentiment for author {author}: {e}")


def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Inserts raw message and triggers the processing function.
    This fulfills the requirement to process ONE MESSAGE AT A TIME.
    """
    STR_PATH = str(db_path)
    author = message["author"]
    
    try:
        # 1. Insert the raw message
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned, message_length
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    message["message"], author, message["timestamp"], 
                    message["category"], message["sentiment"], 
                    message["keyword_mentioned"], message["message_length"],
                ),
            )
            conn.commit()
            logger.info(f"[Raw Data Stored]: Inserted message from {author}")
            
        # 2. Process the result for this single message
        update_author_average_sentiment(author, db_path)
            
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message and update summary: {e}")


#####################################
# Main Consumer Loop
#####################################

def main() -> None:
    """The main function for the continuous Kafka Consumer."""
    logger.info("Starting continuous Kafka Consumer.")
    logger.info("Use Ctrl+C to stop.")

    # STEP 1. Read config

    try:
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        db_path: pathlib.Path = config.get_sqlite_path()
        # --- FIX: Remove the problematic line entirely ---
        # consumer_group: str = config.get_kafka_consumer_group() 
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        return

    # STEP 2. Initialize Database
    init_db(db_path)

    # STEP 3. Setup Kafka Consumer
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            auto_offset_reset='latest',  # Start reading from the latest message
            enable_auto_commit=True,
            # --- FIX: Use a hardcoded string here instead of the missing variable ---
            group_id="sentiment-group-default", 
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"Kafka consumer connected to {kafka_server} on topic '{topic}'")
    except Exception as e:
        logger.error(f"ERROR: Failed to set up Kafka consumer. Is Kafka running? Error: {e}")
        return

    # STEP 4. Continuous Poll Loop
    try:
        for message in consumer:
            # message is a ConsumerRecord object
            payload = message.value
            
            # The core logic: process ONE MESSAGE AT A TIME
            insert_message(payload, db_path)
            
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user (Ctrl+C).")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error in consumer loop: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed. Shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()