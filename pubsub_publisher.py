import json  # Import the json library for handling JSON data

from concurrent import futures  # Import the futures module for handling asynchronous operations
from datetime import datetime  # Import the datetime module for working with dates and times
from google.cloud import pubsub_v1  # Import the Google Cloud Pub/Sub client library
from random import randint  # Import the randint function for generating random integers

# TODO: Replace with your Google Cloud project ID and Pub/Sub topic ID
PROJECT_ID = "optical-fin-435407-e1"
TOPIC_ID = "bike-sharing"

# Create a Publisher client
publisher = pubsub_v1.PublisherClient()
# Define the topic path
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
# List to hold futures for asynchronous publishing
publish_futures = []

def get_callback(publish_future, data):
    # Define a callback function to handle the result of the publish call
    def callback(publish_future):
        try:
            # Wait 60 seconds for the publish call to succeed
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            # Handle the timeout error
            print(f"Publishing {data} timed out.")

    return callback

def create_random_message():
    # Generate random data for the message
    trip_id = randint(10000, 99999)
    start_date = str(datetime.utcnow())
    start_station_id = randint(200, 205)
    bike_number = randint(100, 999)
    duration_sec = randint(1000, 9999)

    # Create a JSON message
    message_json = {
        'trip_id': trip_id,
        'start_date': start_date,
        'start_station_id': start_station_id,
        'bike_number': bike_number,
        'duration_sec': duration_sec
    }
    return message_json

if __name__ == '__main__':
    # Publish 10 messages
    for i in range(10):
        message_json = create_random_message()  # Create a random message
        data = json.dumps(message_json)  # Convert the message to a JSON string
        publish_future = publisher.publish(topic_path, data.encode("utf-8"))  # Publish the message
        publish_future.add_done_callback(get_callback(publish_future, data))  # Add a callback to handle the result
        publish_futures.append(publish_future)  # Add the future to the list

    # Wait for all the publish futures to resolve before exiting
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

    print(f"Published messages with error handler to {topic_path}.")
