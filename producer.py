import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

# Kafka Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'ride-sharing-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

def generate_ride_event():
    """Generates fake ride-sharing events"""
    return {
        "ride_id": fake.uuid4(),
        "driver_id": f"DRV-{random.randint(1000, 9999)}",
        "rider_id": f"RDR-{random.randint(10000, 99990)}",
        "pickup_location": fake.city(),
        "fare": round(random.uniform(5.0, 150.0), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

topic_name = "ride_events"
print("Starting streaming ride events to Kafka... Press Ctrl+C to stop.")

try:
    while True:
        event = generate_ride_event()
        payload = json.dumps(event)
        
        # Send data to Kafka
        producer.produce(topic_name, value=payload, callback=delivery_report)
        producer.poll(0) # Triggers delivery reports
        
        print(f"Sent: {payload}")
        time.sleep(3)

except KeyboardInterrupt:
    print("\nStreaming stopped.")
finally:
    producer.flush()