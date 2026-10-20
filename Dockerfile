FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && \
  apt-get install -y libpcap0.8 tcpdump && \
  rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir scapy kafka-python-ng

# Copy the sniffer script into the container
COPY live_nw_sniffer.py /app/live_nw_sniffer.py

# Run the sniffer
CMD ["python", "-u", "/app/live_nw_sniffer.py"]
