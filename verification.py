import time
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["192.168.29.72:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Ground Truth: BENIGN (Assigned 1.0) - Bidirectional, standard duration
benign_flow = {
    "Destination Port": 443.0,
    "Flow Duration": 50000.0,
    "Total Fwd Packets": 15.0,
    "Total Backward Packets": 16.0,
    "Packet Length Mean": 250.0,
    "Average Packet Size": 250.0,
}

# Ground Truth: DDoS (Assigned 0.0) - Unidirectional SYN flood, high packets
ddos_flow = {
    "Destination Port": 80.0,
    "Flow Duration": 1413228.0,
    "Total Fwd Packets": 3.0,
    "Total Backward Packets": 5.0,
    "Packet Length Mean": 1291.0,
    "Average Packet Size": 1453.4,
}

print("Sending Ground Truth BENIGN flow (Should predict 1.0)...")
producer.send("cic-ids-telemetry", value=benign_flow)
time.sleep(2)

print("Sending Ground Truth DDoS flow (Should predict 0.0)...")
producer.send("cic-ids-telemetry", value=ddos_flow)
producer.flush()
