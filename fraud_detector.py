# fraud_detector.py

from confluent_kafka import Consumer, Producer
from events import PurchaseEvent
import json

# Configurar consumidor
consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'fraud-detector-group',
    'auto.offset.reset': 'earliest'
}

# Configurar productor (por si queremos emitir eventos de fraude)
producer_conf = {
    'bootstrap.servers': 'kafka:9092'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

consumer.subscribe(['purchase-events'])

print("🕵️‍♂️ Módulo de detección de fraudes iniciado...")

def is_fraudulent(event: PurchaseEvent):
    # Lógica simple de ejemplo:
    return event.amount > 1000.0  # Transacciones sospechosas por monto alto

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("⚠️ Error:", msg.error())
            continue

        event = PurchaseEvent.from_json(msg.value().decode())
        print(f"🔎 Verificando evento: {event}")

        if is_fraudulent(event):
            print(f"🚨 FRAUDE DETECTADO para usuario {event.user_id}, monto: {event.amount}")

            # Emitir nuevo evento
            alert = {
                "user_id": event.user_id,
                "reason": "Monto sospechoso",
                "original_event": event.__dict__
            }

            producer.produce(
                topic="fraud-events",
                value=json.dumps(alert).encode('utf-8'),
                callback=lambda err, msg: print("Evento de fraude enviado" if not err else f"Error: {err}")
            )
            producer.flush()

except KeyboardInterrupt:
    print("🛑 Detección de fraudes detenida por el usuario.")
finally:
    consumer.close()
