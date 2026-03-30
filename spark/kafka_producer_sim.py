import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuración del Broker (Ajustar según entorno: localhost:9092 o kafka:29092)
KAFKA_BOOTSTRAP_SERVERS = ['100.31.44.224:9092']
TOPIC_NAME = 'olist_events'
MAX_EVENTS = 10

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def generate_mock_event():
    """Genera un evento de pedido (order) aleatorio basado en el esquema de Olist."""
    order_id = f"ord_{random.randint(1000, 9999)}"
    customer_id = f"cust_{random.randint(100, 999)}"
    
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": random.choice(["delivered", "shipped", "processing"]),
        "event_timestamp": datetime.now().isoformat(),
        "order_value": round(random.uniform(20.0, 500.0), 2)
    }

if __name__ == "__main__":
    print(f"Iniciando Productor de Simulación para el tópico: {TOPIC_NAME}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer
        )
        
        for i in range(MAX_EVENTS):
            event = generate_mock_event()
            print(f"[{i+1}/{MAX_EVENTS}] Enviando evento: {event}")
            producer.send(TOPIC_NAME, event)
            
            # Flush para asegurar envío en tiempo real
            producer.flush()
            
            # Esperar entre 1 y 2 segundos para simular flujo real
            time.sleep(random.uniform(1, 2))
            
    except KeyboardInterrupt:
        print("\nSimulación detenida por el usuario.")
    except Exception as e:
        print(f"Error en el productor: {e}")
