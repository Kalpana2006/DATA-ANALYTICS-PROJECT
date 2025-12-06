# producer/sensor_producer.py
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

locations = [
    {"id": "NH48-01", "lat": 23.0225, "lon": 72.5714},
    {"id": "NH44-02", "lat": 28.7041, "lon": 77.1025},
    {"id": "NH27-03", "lat": 26.9124, "lon": 75.7873}
]

try:
    while True:
        loc = random.choice(locations)
        event = {
            "sensor_id": f"S-{random.randint(1,50)}",
            "location_id": loc['id'],
            "lat": loc['lat'],
            "lon": loc['lon'],
            "vehicle_count": random.randint(0, 120),
            "avg_speed": round(random.uniform(5, 90), 2),
            "timestamp": datetime.utcnow().isoformat() + 'Z'
        }
        producer.send('traffic_stream', value=event)
        print('sent', event)
        time.sleep(1)
except KeyboardInterrupt:
    print('stopped')
    producer.close()
