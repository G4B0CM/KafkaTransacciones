# producer.py

from confluent_kafka import Producer
import json
import random
import time
from EventBaseClass.events import PurchaseEvent

with open("Secrets/config.json", "r") as f:
    config = json.load(f)

producer_conf = config['producer1']
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Error al enviar mensaje: {err}')
    else:
        print(f'Mensaje enviado a {msg.topic()} [{msg.partition()}]')

print("Enviando eventos masivos de compra...")

# Generar 10,000 eventos de prueba
for i in range(10000):
    event = PurchaseEvent(
        user_id=f"user_{random.randint(1, 100)}",
        product_id=f"product_{random.randint(1, 50)}",
        amount=round(random.uniform(5.0, 5000.0), 2),
        status=random.choice(["completed", "pending", "failed"]),
        timestamp=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    )

    producer.produce(
        topic="purchase-events",
        value=event.to_json().encode("utf-8"),
        callback=delivery_report
    )

    sleep_time = random.uniform(0.01, 0.1)  # Simular latencia
    time.sleep(sleep_time)
    
    if i % 1000 == 0:
        producer.flush()

producer.flush()  
print("Todos los eventos han sido enviados. P2")
