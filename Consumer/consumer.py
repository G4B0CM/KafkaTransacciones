# consumer.py

from confluent_kafka import Consumer
from EventBaseClass.events import PurchaseEvent
import json

with open("Secrets/config.json", "r") as f:
    config = json.load(f) 

conf = config['consumer'] 

consumer = Consumer(conf)
consumer.subscribe(['purchase-events'])

# Proyección: base de datos simulada en memoria
purchase_projection = {}

print("Escuchando eventos de compra...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        event = PurchaseEvent.from_json(msg.value().decode())

        print(f"Evento recibido: {event}")

        # Event Sourcing: actualizamos proyección
        user_purchases = purchase_projection.get(event.user_id, [])
        user_purchases.append({
            "product_id": event.product_id,
            "amount": event.amount,
            "status": event.status
        })
        purchase_projection[event.user_id] = user_purchases

        print(f"Estado actualizado de {event.user_id}: {purchase_projection[event.user_id]}")

except KeyboardInterrupt:
    print("Detenido por el usuario.")
finally:
    consumer.close()
