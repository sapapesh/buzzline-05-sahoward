""" sqlite_consumer_sahoward.py 

Has the following functions:
- init_db(config): Initialize the SQLite database and create the 'streamed_messages' table if it doesn't exist.
- insert_message(message, config): Insert a single processed message into the SQLite database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

"""

#####################################
# Import Modules
#####################################

# import from standard library
import os
import pathlib 
import sqlite3
from pathlib import Path

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define Function to Initialize SQLite Database
#####################################


def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database -
    if it doesn't exist, create the 'streamed_messages' table
    and if it does, recreate it.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.

    """
    logger.info("Calling SQLite init_db() with {db_path=}.")
    try:
        # Ensure the directories for the db exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.info("SUCCESS: Got a cursor to execute SQL.")

            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT,
                    message_length INTEGER
                )
            """
            )
            conn.commit()
        logger.info(f"SUCCESS: Database initialized and table ready at {db_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize a sqlite database at {db_path}: {e}")

#####################################
# Define Function to Calculate Average Sentiment
#####################################

def get_average_sentiment_by_author(db_path: pathlib.Path) -> None:
    """
    Calculate and display the average sentiment for each author in the SQLite database.

    Args:
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling get_average_sentiment_by_author() with:")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            
            # SQL query to group by author and calculate the average sentiment
            cursor.execute(
                """
                SELECT
                    author,
                    AVG(sentiment) AS average_sentiment
                FROM
                    streamed_messages
                GROUP BY
                    author
                ORDER BY
                    average_sentiment DESC;
                """
            )
            
            results = cursor.fetchall()
            
            logger.info("--- Average Sentiment by Author ---")
            if results:
                for author, avg_sentiment in results:
                    # Format the sentiment to two decimal places for cleaner output
                    logger.info(f"Author: {author:<10} | Average Sentiment: {avg_sentiment:.2f}")
            else:
                logger.info("No messages found in the database to calculate average sentiment.")
            logger.info("-----------------------------------")

    except Exception as e:
        logger.error(f"ERROR: Failed to retrieve average sentiment by author: {e}")

#####################################
# Define Function to Insert a Processed Message into the Database
#####################################


def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the SQLite database.

    Args:
    - message (dict): Processed message to insert.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    logger.info("Calling SQLite insert_message() with:")
    logger.info(f"{message=}")
    logger.info(f"{db_path=}")

    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
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
            conn.commit()
        logger.info("Inserted one message into the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message into the database: {e}")


#####################################
# Define Function to Delete a Message from the Database
#####################################


def delete_message(message_id: int, db_path: pathlib.Path) -> None:
    """
    Delete a message from the SQLite database by its ID.

    Args:
    - message_id (int): ID of the message to delete.
    - db_path (pathlib.Path): Path to the SQLite database file.
    """
    STR_PATH = str(db_path)
    try:
        with sqlite3.connect(STR_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM streamed_messages WHERE id = ?", (message_id,))
            conn.commit()
        logger.info(f"Deleted message with id {message_id} from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to delete message from the database: {e}")


#####################################
# Define main() function for testing
#####################################
def main():
    logger.info("Starting db testing.")

    # Use config to make a path to a parallel test database
    DATA_PATH: pathlib.path = config.get_base_data_path()
    TEST_DB_PATH: pathlib.Path = DATA_PATH / "test_buzz.sqlite"

    # Initialize the SQLite database by passing in the path
    init_db(TEST_DB_PATH)
    logger.info(f"Initialized database file at {TEST_DB_PATH}.")

    # --- Insert a few test messages ---
    test_messages = [
        {
            "message": "I just shared a meme! It was amazing.",
            "author": "Charlie",
            "timestamp": "2025-01-29 14:35:20",
            "category": "humor",
            "sentiment": 0.87,  # High sentiment
            "keyword_mentioned": "meme",
            "message_length": 42,
        },
        {
            "message": "Python is so confusing today.",
            "author": "Alice",
            "timestamp": "2025-01-29 14:36:00",
            "category": "tech",
            "sentiment": 0.25, # Low sentiment
            "keyword_mentioned": "Python",
            "message_length": 30,
        },
        {
            "message": "Saw a funny movie last night.",
            "author": "Charlie",
            "timestamp": "2025-01-29 14:37:00",
            "category": "entertainment",
            "sentiment": 0.75, # Medium-high sentiment for Charlie
            "keyword_mentioned": "movie",
            "message_length": 34,
        },
        {
            "message": "I love this recipe, so easy!",
            "author": "Alice",
            "timestamp": "2025-01-29 14:38:00",
            "category": "food",
            "sentiment": 0.95, # High sentiment for Alice
            "keyword_mentioned": "recipe",
            "message_length": 30,
        },
    ]

    for msg in test_messages:
        insert_message(msg, TEST_DB_PATH)

    # --- New: Calculate and Display Average Sentiment ---
    get_average_sentiment_by_author(TEST_DB_PATH)

    # --- Clean-up (Optional: Delete all test messages) ---
    try:
        with sqlite3.connect(TEST_DB_PATH) as conn:
            cursor = conn.cursor()
            # Delete all messages from the test run
            cursor.execute("DELETE FROM streamed_messages WHERE author IN ('Charlie', 'Alice')")
            conn.commit()
            logger.info("Cleaned up test messages from the database.")
    except Exception as e:
        logger.error(f"ERROR: Failed to clean up test messages: {e}")

    logger.info("Finished testing.")


# #####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()