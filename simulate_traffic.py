import time
import pandas as pd
from kafka import KafkaProducer


# Callback to confirm successful delivery to Redpanda
def on_send_success(record_metadata):
    print(
        f"Success: topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}"
    )


def on_send_error(excp):
    print(f"Error: Packet delivery failed -> {excp}")


# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=["192.168.29.72:9092"],
    value_serializer=lambda v: v.encode("utf-8"),
    retries=3,  # Add retries for network resilience
)

# Load the slice and clean headers
df = pd.read_csv("test_stream.csv")
df.columns = [c.strip() for c in df.columns]

# Required features matching the ML model
features = [
    "Destination Port",
    "Flow Duration",
    "Total Fwd Packets",
    "Total Backward Packets",
    "Packet Length Mean",
    "Average Packet Size",
]

print(f"Streaming real network packets to Redpanda at 192.168.29.72:9092...")

try:
    for _, row in df[features].iterrows():
        # pandas .to_json() on a Series yields a flat JSON object string
        payload = row.to_json()

        # Send asynchronously with callbacks
        producer.send("cic-ids-telemetry", value=payload).add_callback(
            on_send_success
        ).add_errback(on_send_error)

        time.sleep(0.2)  # Stream 5 packets per second

except KeyboardInterrupt:
    print("\nStreaming interrupted by user.")
finally:
    # Ensure all pending messages are sent before closing
    print("Flushing remaining packets to broker...")
    producer.flush()
    producer.close()
