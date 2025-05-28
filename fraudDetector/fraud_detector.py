from confluent_kafka import Consumer, Producer
from EventBaseClass.events import PurchaseEvent
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import json

with open("Secrets/config.json", "r") as f:
    config = json.load(f)

consumer_conf = config['consumer']
producer_conf = config['producer1']
consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# Suscribirse al topic de compras
consumer.subscribe(['purchase-events'])

print("Módulo de detección de fraudes iniciado...")

# Configuración Azure Service Bus
AZURE_CONNECTION_STR = config['key']
QUEUE_NAME = config['queueName']
sb_client = ServiceBusClient.from_connection_string(conn_str=AZURE_CONNECTION_STR, logging_enable=True)

def is_fraudulent(event: PurchaseEvent):
    return event.amount > 1000.0

try:
    with sb_client:
        sender = sb_client.get_queue_sender(queue_name=QUEUE_NAME)

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Error:", msg.error())
                continue

            event = PurchaseEvent.from_json(msg.value().decode())
            print(f"Verificando evento: {event}")

            if is_fraudulent(event):
                print(f"🚨 FRAUDE DETECTADO para usuario {event.user_id}, monto: {event.amount}")
    
                alert = {
                    "user_id": event.user_id,
                    "reason": "Monto sospechoso",
                    "original_event": event.__dict__
                }

                # Crea un nuevo sender y úsalo solo en ese contexto
                with sb_client:
                    sender = sb_client.get_queue_sender(queue_name=QUEUE_NAME)
                    with sender:
                        message = ServiceBusMessage(json.dumps(alert))
                        sender.send_messages(message)
                        print("Evento enviado a Azure Service Bus")

except KeyboardInterrupt:
    print("Detección de fraudes detenida por el usuario.")
finally:
    consumer.close()
    