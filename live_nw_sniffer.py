import time
import json
from collections import defaultdict
from scapy.all import sniff, IP, TCP, UDP, conf
from kafka import KafkaProducer

BROKER = "192.168.29.72:9092"
TOPIC = "cic-ids-telemetry"

producer = KafkaProducer(
    bootstrap_servers=[BROKER], value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

flows = defaultdict(lambda: [time.time(), time.time(), 0, 0, []])


def process_packet(packet):
    if IP in packet and (TCP in packet or UDP in packet):
        proto = "TCP" if TCP in packet else "UDP"
        src_ip, dst_ip = packet[IP].src, packet[IP].dst
        src_port, dst_port = packet.sport, packet.dport

        if src_ip < dst_ip:
            flow_key = (src_ip, dst_ip, src_port, dst_port, proto)
            direction = "forward"
        else:
            flow_key = (dst_ip, src_ip, dst_port, src_port, proto)
            direction = "backward"

        flow = flows[flow_key]
        flow[1] = time.time()

        if direction == "forward":
            flow[2] += 1
        else:
            flow[3] += 1

        flow[4].append(len(packet))

        total_packets = flow[2] + flow[3]
        if total_packets % 10 == 0:
            duration = max(flow[1] - flow[0], 0.001) * 1000000
            lengths = flow[4]
            mean_len = float(sum(lengths) / len(lengths))

            payload = {
                "Destination Port": float(flow_key[3]),
                "Flow Duration": float(duration),
                "Total Fwd Packets": float(flow[2]),
                "Total Backward Packets": float(flow[3]),
                "Packet Length Mean": mean_len,
                "Average Packet Size": mean_len,
            }
            producer.send(TOPIC, value=payload)
            print(
                f"[{conf.iface}] Port: {flow_key[3]} | Fwd: {flow[2]} | Bwd: {flow[3]}",
                flush=True,
            )


# Exclude internal K3s API (6443), Flannel VXLAN (8472), Kubelet (10250), Prometheus (9100), and Redpanda (9092)
bpf_filter = (
    "not (port 9092 or port 6443 or port 8472 or port 10250 or port 9100 or port 38080)"
)

print(f"[*] Edge Sniffer active on interface: {conf.iface}", flush=True)
sniff(iface=conf.iface, filter=bpf_filter, prn=process_packet, store=False)
