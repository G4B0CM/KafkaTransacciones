# 🛡️ Fraud Detection System with Kafka & Azure Service Bus

This project implements a real-time fraud detection system using **Apache Kafka** as the event streaming platform and integrates with **Azure Service Bus** for alert dispatching.

The system is designed to detect potentially fraudulent transactions by analyzing purchase events and triggering alerts based on predefined rules.

---

## 📦 Project Structure

KafkaTransacciones/
├── Producers/
│ ├── producer.py
│ ├── producer1.py
│ └── producer2.py
├── Consumer/
│ └── consumer.py
├── fraudDetector/
│ └── fraud_detector.py
├── EventBaseClass/
│ └── events.py
├── docker-compose.yml
├── Dockerfile
└── config.json (🔒 not included in repo)

yaml
Copiar
Editar

---

## ⚙️ Technologies Used

- **Python 3.11**
- **Apache Kafka** (via Docker with Zookeeper)
- **Confluent Kafka Python Client**
- **Azure Service Bus** (in the `Service-Bus` branch)
- **Docker & Docker Compose**

---

## 🚀 How It Works

1. **Producers** send simulated purchase events to the Kafka topic `purchase-events`.
2. **Consumer** listens for and logs all purchase events.
3. **Fraud Detector** consumes events, applies fraud detection rules, and:
   - Sends valid events to `fraud-events` (Kafka).
   - Triggers fraud alerts via Azure Service Bus (`fraude` queue) – available in the `Service-Bus` branch.

## 🛠️ Running the Project

### 1. Clone the repository


git clone https://github.com/your-username/KafkaTransacciones.git
cd KafkaTransacciones
2. Build and run the services with Docker Compose
bash
Copiar
Editar
docker-compose up --build
3. Verify logs
Watch the output of the services in the terminal or use:

bash
Copiar
Editar
docker logs fraud_detector
docker logs consumer
docker logs producer
🔐 Configuration
The project requires a config.json file placed in the project root directory to load Kafka and Azure Service Bus credentials.

This file is intentionally excluded from the repository for security reasons.

Expected structure:

json
Copiar
Editar
{
  "key": "Azure_Service_Bus_Connection_String",
  "queueName": "fraude",
  "producer1": {
    "bootstrap.servers": "kafka:9092"
  },
  "consumer": {
    "bootstrap.servers": "kafka:9092",
    "group.id": "purchase-consumer-group",
    "auto.offset.reset": "earliest"
  }
}

Service Bus Integration
The integration with Azure Service Bus is available in the branch:

Copiar
Editar
Service-Bus
To switch:

bash
Copiar
Editar
git checkout Service-Bus
This branch includes:

Azure SDK for Python

Sending fraud alerts to Azure Service Bus queue: fraude

📌 Notes
Ensure your config.json contains valid Kafka and Azure credentials.

Topics used: purchase-events, fraud-events.

Messages are JSON-encoded.

Fraud is determined by simple rule-based logic (amount > 1000.0).

📫 Contact
For questions, suggestions, or contributions, feel free to open an issue or pull request!
