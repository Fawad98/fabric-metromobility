import json, os, random, time, uuid
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

CONN_STR = os.environ["FABRIC_ES_CONN"]
EH_NAME = os.environ["FABRIC_ES_NAME"]

ZONES = [4, 13, 79, 88, 100, 132, 138, 141, 161, 186, 230, 236, 237, 263]
STATUSES = ["requested", "accepted", "en_route", "completed", "cancelled"]

producer = EventHubProducerClient.from_connection_string(CONN_STR, eventhub_name=EH_NAME)

print("streaming ride telemetry... Ctrl+C to stop")
try:
    while True:
        batch = producer.create_batch()
        for _ in range(random.randint(5, 20)):
            surge = round(random.choices([1.0, 1.2, 1.5, 2.0, 2.5],
                                         weights=[60, 15, 12, 8, 5])[0], 1)
            event = {
                "ride_id": str(uuid.uuid4()),
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "pickup_zone_id": random.choice(ZONES),
                "dropoff_zone_id": random.choice(ZONES),
                "status": random.choices(STATUSES, weights=[25, 20, 20, 30, 5])[0],
                "surge_multiplier": surge,
                "eta_minutes": random.randint(2, 25),
                "fare_estimate": round(random.uniform(8, 90) * surge, 2),
            }
            batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)
        print(f"{datetime.now():%H:%M:%S} sent batch")
        time.sleep(2)
except KeyboardInterrupt:
    producer.close()
    print("stopped")