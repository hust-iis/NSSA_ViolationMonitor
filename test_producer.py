from pyshark import LiveCapture
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pickle
from pyshark import FileCapture

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# capture = LiveCapture(interface="en0",  bpf_filter="tcp")
capture = FileCapture("./123.pcap", display_filter="http")
producer.flush()
for pkt in capture:
    pkg = pickle.dumps(pkt)
    future = producer.send('net-traffic', key=b'my_key', value=pkg)
    # result = future.get(timeout=10)
