import time
from collections import defaultdict
from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer
import json

BROKER = "192.168.29.72:9092"
TOPIC = "cic-ids-telemetry"

producer = KafkaProducer(
    bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Flow tracker
flows = defaultdict(lambda: [time.time(), time.time(), 0, 0, []])


def process_packet(packet):
    if IP in packet and (TCP in packet or UDP in packet):
        proto = "TCP" if TCP in packet else "UDP"

        src_ip = packet[IP].src
        dst_ip = packet[IP].dst
        src_port = packet.sport
        dst_port = packet.dport

        # 1. FIX BIDIRECTIONAL FLOWS:
        # Sort IPs so A->B and B->A always hash to the exact same dictionary key
        if src_ip < dst_ip:
            flow_key = (src_ip, dst_ip, src_port, dst_port, proto)
            direction = "forward"
        else:
            flow_key = (dst_ip, src_ip, dst_port, src_port, proto)
            direction = "backward"

        flow = flows[flow_key]
        flow[1] = time.time()  # Update last seen time

        if direction == "forward":
            flow[2] += 1
        else:
            flow[3] += 1

        flow[4].append(len(packet))

        # 2. PREVENT KAFKA FLOODING:
        # Only emit a prediction request every 10 packets to avoid overwhelming Spark
        total_packets = flow[2] + flow[3]
        if total_packets % 10 == 0:
            duration = max(flow[1] - flow[0], 0.001) * 1000000
            lengths = flow[4]

            payload = {
                "Destination Port": float(flow_key[3]),  # Canonical dest port
                "Flow Duration": float(duration),
                "Total Fwd Packets": float(flow[2]),
                "Total Backward Packets": float(flow[3]),
                "Packet Length Mean": float(sum(lengths) / len(lengths)),
                "Average Packet Size": float(sum(lengths) / len(lengths)),
            }

            producer.send(TOPIC, value=payload)
            print(
                f"Sent Flow | Port: {flow_key[3]} | FwdPkts: {flow[2]} | BwdPkts: {flow[3]}"
            )


print(f"[*] Sniffing bidirectional traffic (ignoring internal telemetry)...")

# 3. KERNEL BPF FILTER: Stop the infinite loop before Python even sees the packets
# Ignore Redpanda (9092) and Spark BlockManager (38080) traffic
bpf_filter = "not (port 9092 or port 38080)"

sniff(iface="eno1", filter=bpf_filter, prn=process_packet, store=False)
