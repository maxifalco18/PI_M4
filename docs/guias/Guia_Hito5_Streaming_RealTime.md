# GUÍA DEFINITIVA: HITO 5 (STREAMING EN TIEMPO REAL CON KAFKA)
**Nivel:** Data Engineer Intermedio / Senior
**Objetivo:** Implementar una Arquitectura Lambda completa integrando eventos en tiempo real desde Kafka hacia nuestro Data Lake, unificándolos con el procesamiento Batch.

En esta fase, dejamos de ser "procesadores de archivos" para convertirnos en "arquitectos de eventos". Usaremos **Apache Kafka** como nuestro sistema nervioso central y **Spark Structured Streaming** para capturar la realidad mientras sucede.

---

## FASE A: El Sistema Nervioso (Kafka en EC2)

Kafka no es solo una base de datos; es un log de mensajes inmutable. En este proyecto, corre dentro de la misma EC2 que Airflow bajo Docker Compose.

### Paso 1: Entender el Topic
1. Nuestro productor (el origen) envía mensajes al topic `olist_events`.
2. Cada mensaje es un JSON que representa una orden en tiempo real.
3. Como ingenieros senior, usamos un **Schema Registry**. Esto asegura que si alguien envía un mensaje con formato roto, el sistema lo rechace *antes* de ensuciar el Data Lake.

---

## FASE B: Captura de Eventos (Speed Layer)

Crearemos un Job de Spark que nunca termina (Streaming). A diferencia de los jobs anteriores, este se queda escuchando a Kafka.

### Paso 2: El Script de Captura (`stream_raw_to_processed.py`)
Este job hace tres cosas críticas:
1. **Consumir**: Se conecta a Kafka (`boostrap.servers`).
2. **Validar**: Cruza el mensaje contra el AWS Glue Schema Registry.
3. **Persistir**: Guarda en S3 en formato Parquet, usando **Watermarking** para manejar retrasos de red.

```python
# Un vistazo al corazón del Streaming:
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "olist_events") \
    .load()

# Aplicamos Watermarking de 2 horas (Senior Pattern)
# Esto permite que datos que lleguen tarde se procesen correctamente.
df_clean = df_stream.withWatermark("event_timestamp", "2 hours")
```

---

## FASE C: La Convergencia (Arquitectura Lambda)

El mayor desafío: ¿Cómo evitamos duplicados si una orden llega por Streaming ahora y por Batch mañana?

### Paso 3: Unificación en Capa Gold
En `silver_to_gold.py`, implementamos la función de **Lambda Fusion**.
1. Leemos los datos históricos (Batch).
2. Leemos los datos recientes (Speed Layer).
3. Hacemos un `UNION` y aplicamos un `dropDuplicates` basado en el ID de la orden.

**Resultado**: El analista ve una sola tabla `gold_sales` que tiene lo que pasó hace un año y lo que pasó hace 30 segundos.

---

## FASE D: Monitoreo y Alerta

El streaming es frágil. Si el broker de Kafka se cae, el pipeline se detiene.
1. **Checkpoints**: Usamos una carpeta en S3 (`/checkpoints/`) para que si el job falla, Spark sepa exactamente en qué mensaje se quedó.
2. **Alertas Proactivas**: Si el Job de Streaming se detiene por error, Airflow (tu orquestador) te enviará un mensaje inmediato a **Slack**.

**¡Felicidades! Has pasado de Procesamiento por Lotes a una Plataforma de Datos Predictiva y Reactiva.**
