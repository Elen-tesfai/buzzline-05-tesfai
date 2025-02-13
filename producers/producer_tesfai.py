"""
producer_tesfai.py
This script generates a stream of JSON messages simulating producer messages and writes them to a JSON file.
"""

import json
import random
import time
from datetime import datetime
import pathlib
import os

# Assuming utils_config and logger are already imported

def assess_sentiment(text: str) -> float:
    """Stub for sentiment analysis."""
    # You can replace this with an actual sentiment analysis library
    return round(random.uniform(0, 1), 2)

def generate_messages():
    """Generate a stream of JSON messages."""
    ADJECTIVES = ["amazing", "funny", "boring", "exciting", "weird"]
    ACTIONS = ["found", "saw", "tried", "shared", "loved"]
    TOPICS = ["a movie", "a meme", "an app", "a trick", "a story", "Python", "JavaScript", "recipe", "travel", "game"]
    AUTHORS = ["Alice", "Bob", "Charlie", "Eve"]
    KEYWORD_CATEGORIES = {
        "meme": "humor", "Python": "tech", "JavaScript": "tech", "recipe": "food", 
        "travel": "travel", "movie": "entertainment", "game": "gaming"
    }

    while True:
        adjective = random.choice(ADJECTIVES)
        action = random.choice(ACTIONS)
        topic = random.choice(TOPICS)
        author = random.choice(AUTHORS)
        message_text = f"I just {action} {topic}! It was {adjective}."
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        keyword_mentioned = next((word for word in KEYWORD_CATEGORIES if word in topic), "other")
        category = KEYWORD_CATEGORIES.get(keyword_mentioned, "other")
        
        sentiment = assess_sentiment(message_text)
        
        json_message = {
            "message": message_text,
            "author": author,
            "timestamp": timestamp,
            "category": category,
            "sentiment": sentiment,
            "keyword_mentioned": keyword_mentioned,
            "message_length": len(message_text),
        }

        yield json_message

def main():
    try:
        # Step 1: Get live data path (usually project_live.json)
        live_data_path = pathlib.Path('C:/Users/su_te/buzzline-05-tesfai/data/project_live.json')

        # Step 2: Build the path folders to the live data file if needed
        os.makedirs(live_data_path.parent, exist_ok=True)

        # Step 3: Generate messages continuously
        for message in generate_messages():
            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                print(f"Message written to {live_data_path}: {message}")
            time.sleep(2)  # Adjust the sleep time if needed

    except KeyboardInterrupt:
        print("Process interrupted by user.")
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()