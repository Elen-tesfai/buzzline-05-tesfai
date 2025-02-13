import pathlib
import sqlite3
import random
import time
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from utils.utils_logger import logger  # Make sure you have the logger in place for better tracking

# Define a dictionary for category selection based on keywords
KEYWORD_CATEGORIES = {
    "meme": "humor",
    "Python": "tech",
    "JavaScript": "tech",
    "recipe": "food",
    "travel": "travel",
    "movie": "entertainment",
    "game": "gaming"
}

def assess_sentiment(text: str) -> float:
    """Stub for sentiment analysis."""
    return round(random.uniform(0, 1), 2)


def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """Insert a processed message into the SQLite database."""
    logger.info(f"Inserting message into database: {message}")
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
                VALUES (?, ?, ?, ?, ?, ?, ?)
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
        logger.info("Message inserted successfully.")
    except Exception as e:
        logger.error(f"Failed to insert message into the database: {e}")


def fetch_data_from_db(db_path: pathlib.Path) -> list:
    """Fetch the processed messages from the SQLite database."""
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM streamed_messages")
            rows = cursor.fetchall()
            logger.info(f"Fetched {len(rows)} messages from the database.")
            return rows
    except Exception as e:
        logger.error(f"Error fetching data from the database: {e}")
        return []


def visualize_sentiment(data: list) -> None:
    """Visualize the sentiment of messages by category."""
    if not data:
        logger.warning("No data to visualize.")
        return

    # Prepare the data for visualization
    sentiment_data = {
        "message": [],
        "author": [],
        "timestamp": [],
        "category": [],
        "sentiment": [],
        "keyword_mentioned": [],
        "message_length": []
    }

    for row in data:
        sentiment_data["message"].append(row[1])
        sentiment_data["author"].append(row[2])
        sentiment_data["timestamp"].append(row[3])
        sentiment_data["category"].append(row[4])
        sentiment_data["sentiment"].append(row[5])
        sentiment_data["keyword_mentioned"].append(row[6])
        sentiment_data["message_length"].append(row[7])

    # Convert to DataFrame
    df = pd.DataFrame(sentiment_data)

    # Log the data for debugging
    logger.info(f"Fetched Data:\n{df.head()}")  # Log the first few rows for debugging

    if df.empty:
        logger.warning("Dataframe is empty. No data to plot.")
        return

    # Plot the sentiment by category
    plt.figure(figsize=(10, 6))
    sns.boxplot(x="category", y="sentiment", data=df)
    plt.title("Sentiment by Category")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot as an image
    plt.savefig('sentiment_by_category.png')
    logger.info("Plot saved as 'sentiment_by_category.png'")

    # Show the plot
    plt.show()


def main():
    # Path to your SQLite database
    db_path = pathlib.Path('C:/Users/su_te/buzzline-05-tesfai/data/buzz.sqlite')
    logger.info(f"Using database at: {db_path}")

    # Categories list for generating diverse categories
    categories = ["humor", "tech", "food", "travel", "entertainment", "gaming"]

    # Generate messages (simulating the consumer behavior)
    for i in range(5):  # Just sending 5 messages as an example
        # Randomly pick a category from the list
        category = random.choice(categories)
        keyword = category  # Simplified, use category as keyword for this example

        message = {
            "message": f"I just shared a {keyword}! It was amazing! {i}",
            "author": f"Author {i}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "category": category,
            "sentiment": assess_sentiment(f"I just shared a {keyword}! It was amazing! {i}"),
            "keyword_mentioned": keyword,
            "message_length": len(f"I just shared a {keyword}! It was amazing! {i}")
        }

        # Insert message into the database
        insert_message(message, db_path)
        time.sleep(1)  # Simulate delay between messages

    # Fetch data from database for visualization
    data = fetch_data_from_db(db_path)

    # Visualize the sentiment data
    visualize_sentiment(data)


if __name__ == "__main__":
    main()