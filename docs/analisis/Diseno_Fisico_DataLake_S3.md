# 🏗️ Diseño Físico del Data Lake en AWS S3
## Debate Multi-Agente — Proyecto Integrador M4

**Bucket:** `s3://pi-m4-datalake-maxi`  
**Región:** `us-east-1`  
**Pipeline:** ETLT (Batch diario + Streaming continuo)  
**Fecha:** 2026-03-24  

---

## 👥 Presentación del Equipo

### 🔵 Agente 1 — Arquitecto Cloud y Data Lake ("El Organizador")
> Soy el responsable de diseñar la estructura lógica del lago en capas Medallion (Raw → Silver → Gold). Mi foco está en la separación clara entre flujos Batch y Streaming, la gobernanza de los datos, y la gestión del ciclo de vida para mantener los costos dentro del Free Tier de AWS.

### 🟢 Agente 2 — Especialista en Particionamiento y Rendimiento ("El Optimizador")
> Mi trabajo es que Spark y Athena lean solo lo que necesitan. Diseño el particionamiento físico de los directorios para evitar full-scans y el problema de Small Files. Si una partición tiene demasiadas carpetas o archivos diminutos, pago con latencia y OOM. Cada decisión de `partitionBy` y `coalesce` pasa por mí.

### 🔴 Agente 3 — Experto en Formatos y Compresión ("El Eficiente")
> Decido qué formato y compresión usar en cada capa. No es lo mismo guardar un JSON crudo que servir una tabla Gold a un dashboard. Mi objetivo: minimizar bytes escaneados (= costos en Athena) y maximizar la velocidad de lectura columnar, permitiendo transacciones ACID donde el pipeline lo requiera.

---

## 🗣️ Debate Inter-Agentes

### Ronda 1: Estructura base del lago

**🔵 Organizador:** Propongo la siguiente separación de alto nivel. El bucket tiene **5 prefijos raíz**:

| Prefijo | Propósito |
|---------|-----------|
| `raw/batch/` | Datos crudos de ingesta diaria (Airbyte → PostgreSQL, API JSON) |
| `raw/streaming/` | Datos crudos de Kafka (eventos de clima en tiempo real) |
| `processed/batch/` | Capa Silver: datos limpios y modelados del batch |
| `processed/streaming/` | Capa Silver: eventos de streaming limpiados |
| `gold/` | Capa Gold unificada: tablas analíticas que cruzan batch + streaming |

> **Justificación:** Separar `batch` de `streaming` dentro de Raw y Processed evita que un job de Spark leyendo batch pise accidentalmente particiones que están siendo escritas por Structured Streaming. Son dos cadencias distintas (diaria vs. continua) y mezclarlas genera conflictos de escritura y lecturas sucias.

**🟢 Optimizador:** Estoy de acuerdo. Agrego que la capa Gold **no** se separa por batch/streaming porque es la capa de convergencia. Ahí los datos ya están reconciliados.

**🔴 Eficiente:** Correcto. Y eso impacta mi elección de formato: la capa Gold necesita soportar **merges** entre datos batch (diarios) y streaming (continuos). Esto me obliga a usar Delta Lake en Gold para tener transacciones ACID.

---

### Ronda 2: Particionamiento

**🟢 Optimizador:** Aquí es donde se juega el rendimiento. Mi propuesta:

#### Capa Raw (Batch)
- **Sin particionamiento manual.** Airbyte deposita los archivos con su estructura propia (`raw/batch/{table_name}/`). Son datos inmutables — la "fuente de la verdad".
- No creo carpetas `year=`/`month=` aquí porque Airbyte hace Full Refresh o Append sin ese control.

#### Capa Raw (Streaming)
- **Partición por fecha + hora:** `raw/streaming/weather_events/year=YYYY/month=MM/day=DD/hour=HH/`
- **¿Por qué hora y no minuto?** Con un volumen de ~100-500 eventos/hora de OpenWeatherMap, particionar por minuto generaría **60 carpetas por hora**, cada una con archivos de ~1-5 KB. Eso es el **Small Files Problem** clásico: el NameNode (o el listing de S3) se satura, y cada `spark.read` tiene que hacer miles de API calls `ListObjects`. Particionando por hora, acumulo suficientes registros para que cada archivo tenga un tamaño razonable (64-128 KB mínimo).

**🔴 Eficiente:** Confirmo. Un archivo Parquet de menos de 1 MB no aprovecha la lectura columnar. El row-group por defecto es 128 MB, pero con files de 5 KB ni siquiera se forma un row-group completo. Hora es el granulo ideal.

#### Capa Processed/Silver (Batch)
| Tabla | Tipo | Partición | Justificación |
|-------|------|-----------|---------------|
| `fact_orders` | Hecho | `year=YYYY/month=MM` | Filtra el 90% de queries por rango temporal |
| `fact_order_items` | Hecho | `year=YYYY/month=MM` | Hereda timestamp de la orden |
| `fact_payments` | Hecho | `payment_type=X` | 5 valores únicos → 5 carpetas (credit_card, boleto, voucher, debit_card, not_defined) |
| `fact_reviews` | Hecho | `review_score=N` | 5 valores (1-5) → permite análisis de satisfacción segmentado |
| `dim_customers` | Dimensión | Sin partición | ~99K registros, tabla pequeña → broadcast join |
| `dim_products` | Dimensión | Sin partición | ~33K registros |
| `dim_sellers` | Dimensión | Sin partición | ~3K registros |
| `dim_categories` | Dimensión | Sin partición | ~71 registros (traducciones) |
| `dim_geolocation` | Dimensión | `geolocation_state=XX` | Excepción: 1M+ registros, 27 estados → reduce scan 96% |
| `weather_enriched` | Enriquecida | Sin partición | Dataset pequeño (~7 MB limpio) |

**🔵 Organizador:** Pregunta para el Optimizador — ¿por qué `fact_payments` por `payment_type` y no por fecha?

**🟢 Optimizador:** Porque las preguntas de negocio sobre pagos son del tipo *"¿Cuánto facturamos con boleto vs. tarjeta de crédito?"*. El filtro principal es el método, no la fecha. Además, con solo 5 valores posibles, genero exactamente 5 carpetas — ideal. Si lo particionara por `year/month`, generaría ~24 carpetas (2 años × 12 meses) con poca data en cada una.

#### Capa Processed/Silver (Streaming)
| Tabla | Partición | Justificación |
|-------|-----------|---------------|
| `weather_events_clean` | `year=YYYY/month=MM/day=DD/hour=HH` | Mantiene granularidad horaria para joins temporales con batch |

#### Capa Gold (Unificada)
| Tabla Gold | Partición | Pregunta de negocio |
|------------|-----------|---------------------|
| `gold_sales_by_category_time` | `year, month` | Productos más vendidos por categoría y tiempo |
| `gold_customer_frequency` | Sin partición | Frecuencia de compra y ticket promedio |
| `gold_revenue_by_region` | `customer_state` | Regiones con mayores ingresos |
| `gold_new_vs_returning` | `year, month` | Clientes nuevos vs recurrentes por período |
| `gold_price_vs_volume` | Sin partición | Relación precio-volumen por categoría |
| `gold_payment_performance` | `payment_type` | Desempeño por método de pago |
| `gold_product_profitability` | Sin partición | Márgenes de rentabilidad por producto |
| `gold_weather_impact` | `year, month` | Impacto del clima en las ventas (cruce batch+streaming) |

**🔴 Eficiente:** `gold_weather_impact` es la tabla donde converge batch con streaming. Necesita Delta Lake obligatoriamente para hacer `MERGE INTO` sin duplicados.

---

### Ronda 3: Formatos y Compresión

**🔴 Eficiente:** Mi recomendación por capa:

| Capa | Formato | Compresión | Justificación |
|------|---------|------------|---------------|
| **Raw/Batch** | Parquet | Snappy | Airbyte ya está configurado para escribir Parquet. Mantener coherencia. Si la fuente fuera CSV/JSON puro, se guardaría tal cual con GZIP, pero Airbyte hace la conversión. |
| **Raw/Streaming** | JSON → Parquet | GZIP → Snappy | Kafka produce JSON (schema mínimo). El micro-batch de Structured Streaming lo convierte a Parquet Snappy al aterrizar. |
| **Processed/Silver** | Parquet | Snappy | Formato columnar estándar. Snappy prioriza velocidad de descompresión sobre ratio (trade-off ideal para queries interactivas). |
| **Gold** | **Delta Lake** (Parquet + Transaction Log) | Snappy | Delta es Parquet + un `_delta_log/` que da ACID. Permite `MERGE INTO` para upserts cuando el streaming actualiza datos que el batch ya escribió. Time Travel gratis para auditoría. |

**🟢 Optimizador:** Una nota sobre Delta en Gold: el `_delta_log/` agrega metadatos por cada transacción. Necesitamos correr `OPTIMIZE` periódicamente para compactar los Small Files que genera el streaming (cada micro-batch crea un archivo). Sin `OPTIMIZE`, volvemos al Small Files Problem.

**🔵 Organizador:** Perfecto. Agrego eso como una tarea de mantenimiento en el DAG de Airflow: un task `optimize_gold_tables` que corra después de cada ingesta batch diaria.

---

## 🌳 Árbol de Directorios Definitivo

```
s3://pi-m4-datalake-maxi/
│
├── raw/
│   ├── batch/                                    ← Airbyte deposits (inmutables)
│   │   ├── olist_customers/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── olist_orders/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── olist_order_items/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── olist_order_payments/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── olist_order_reviews/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── olist_products/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── olist_sellers/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── olist_geolocation/
│   │   │   └── part-00000.snappy.parquet
│   │   ├── product_category_translation/
│   │   │   └── part-00000.snappy.parquet
│   │   └── api_weather/
│   │       └── weather_history.json.gz
│   │
│   └── streaming/                                ← Kafka → Structured Streaming
│       └── weather_events/
│           └── year=2026/
│               └── month=03/
│                   └── day=24/
│                       ├── hour=10/
│                       │   └── part-00000.snappy.parquet    (~50-200 KB)
│                       ├── hour=11/
│                       │   └── part-00000.snappy.parquet
│                       └── hour=12/
│                           └── part-00000.snappy.parquet
│
├── processed/
│   ├── batch/                                    ← PySpark ETL (Silver)
│   │   ├── fact_orders/
│   │   │   └── year=2018/
│   │   │       └── month=06/
│   │   │           ├── part-00000.snappy.parquet
│   │   │           ├── part-00001.snappy.parquet
│   │   │           ├── part-00002.snappy.parquet
│   │   │           └── part-00003.snappy.parquet    (4 files × ~2-5 MB c/u)
│   │   ├── fact_order_items/
│   │   │   └── year=2018/
│   │   │       └── month=06/
│   │   │           └── part-00000.snappy.parquet
│   │   ├── fact_payments/
│   │   │   ├── payment_type=credit_card/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── payment_type=boleto/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── payment_type=voucher/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── payment_type=debit_card/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   └── payment_type=not_defined/
│   │   │       └── part-00000.snappy.parquet
│   │   ├── fact_reviews/
│   │   │   ├── review_score=1/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── review_score=2/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── review_score=3/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── review_score=4/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   └── review_score=5/
│   │   │       └── part-00000.snappy.parquet
│   │   ├── dim_customers/
│   │   │   └── part-00000.snappy.parquet           (archivo único, ~5 MB)
│   │   ├── dim_products/
│   │   │   └── part-00000.snappy.parquet           (archivo único, ~3 MB)
│   │   ├── dim_sellers/
│   │   │   └── part-00000.snappy.parquet           (archivo único, ~1 MB)
│   │   ├── dim_categories/
│   │   │   └── part-00000.snappy.parquet           (archivo único, ~10 KB)
│   │   ├── dim_geolocation/
│   │   │   ├── geolocation_state=SP/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── geolocation_state=RJ/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   ├── geolocation_state=MG/
│   │   │   │   └── part-00000.snappy.parquet
│   │   │   └── ... (27 estados)
│   │   └── weather_enriched/
│   │       └── part-00000.snappy.parquet           (archivo único, ~2 MB)
│   │
│   └── streaming/                                ← Streaming limpio (Silver)
│       └── weather_events_clean/
│           └── year=2026/
│               └── month=03/
│                   └── day=24/
│                       └── hour=12/
│                           └── part-00000.snappy.parquet
│
└── gold/                                         ← Delta Lake (ACID)
    ├── gold_sales_by_category_time/
    │   ├── _delta_log/
    │   │   ├── 00000000000000000000.json
    │   │   └── 00000000000000000001.json
    │   └── year=2018/
    │       └── month=06/
    │           └── part-00000.snappy.parquet
    ├── gold_customer_frequency/
    │   ├── _delta_log/
    │   │   └── 00000000000000000000.json
    │   └── part-00000.snappy.parquet
    ├── gold_revenue_by_region/
    │   ├── _delta_log/
    │   │   └── 00000000000000000000.json
    │   ├── customer_state=SP/
    │   │   └── part-00000.snappy.parquet
    │   └── customer_state=RJ/
    │       └── part-00000.snappy.parquet
    ├── gold_new_vs_returning/
    │   ├── _delta_log/
    │   │   └── 00000000000000000000.json
    │   └── year=2018/
    │       └── month=06/
    │           └── part-00000.snappy.parquet
    ├── gold_price_vs_volume/
    │   ├── _delta_log/
    │   │   └── 00000000000000000000.json
    │   └── part-00000.snappy.parquet
    ├── gold_payment_performance/
    │   ├── _delta_log/
    │   │   └── 00000000000000000000.json
    │   ├── payment_type=credit_card/
    │   │   └── part-00000.snappy.parquet
    │   └── payment_type=boleto/
    │       └── part-00000.snappy.parquet
    ├── gold_product_profitability/
    │   ├── _delta_log/
    │   │   └── 00000000000000000000.json
    │   └── part-00000.snappy.parquet
    └── gold_weather_impact/
        ├── _delta_log/
        │   ├── 00000000000000000000.json
        │   └── 00000000000000000001.json     ← MERGE batch+streaming
        └── year=2026/
            └── month=03/
                └── part-00000.snappy.parquet
```

---

## ⚙️ Configuraciones Clave de PySpark

```python
# ==============================================================================
# CONFIGURACIÓN PYSPARK — DISEÑO FÍSICO DEL DATA LAKE
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = (SparkSession.builder
    .appName("PI_M4_DataLake_ETL")
    
    # ---------- Delta Lake ----------
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # ---------- S3 Access ----------
    .config("spark.hadoop.fs.s3a.impl", 
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    
    # ---------- Rendimiento ----------
    # Shuffle Partitions: 8 en vez de 200 (ideal para ~100K-1M registros)
    .config("spark.sql.shuffle.partitions", "8")
    
    # Adaptive Query Execution: deja que Spark ajuste particiones dinámicamente
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
    
    # Máximo registros por archivo de salida (evita archivos enormes)
    .config("spark.sql.files.maxRecordsPerFile", "500000")
    
    # Tamaño máximo para broadcast automático (10 MB)
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
    
    # Parquet: compresión Snappy
    .config("spark.sql.parquet.compression.codec", "snappy")
    
    # ---------- Memoria (Free Tier / t2.micro con 1 GB RAM) ----------
    .config("spark.driver.memory", "512m")
    .config("spark.executor.memory", "512m")
    .config("spark.memory.fraction", "0.8")
    .config("spark.memory.storageFraction", "0.3")
    
    .getOrCreate()
)

# ==============================================================================
# CONSTANTES DE RUTAS S3
# ==============================================================================
BUCKET = "s3a://pi-m4-datalake-maxi"

# --- Raw ---
RAW_BATCH      = f"{BUCKET}/raw/batch"
RAW_STREAMING  = f"{BUCKET}/raw/streaming"

# --- Processed/Silver ---
SILVER_BATCH     = f"{BUCKET}/processed/batch"
SILVER_STREAMING = f"{BUCKET}/processed/streaming"

# --- Gold ---
GOLD = f"{BUCKET}/gold"

# ==============================================================================
# EJEMPLO: ESCRIBIR fact_orders PARTICIONADO
# ==============================================================================
df_orders = (spark.read.parquet(f"{RAW_BATCH}/olist_orders")
    .withColumn("year", F.year("order_purchase_timestamp"))
    .withColumn("month", F.month("order_purchase_timestamp"))
)

(df_orders
    .coalesce(4)                         # 4 archivos por partición (no repartition → sin shuffle)
    .write
    .partitionBy("year", "month")        # Particionamiento jerárquico
    .mode("overwrite")
    .parquet(f"{SILVER_BATCH}/fact_orders")
)

# ==============================================================================
# EJEMPLO: BROADCAST JOIN CON DIMENSIONES
# ==============================================================================
df_products = spark.read.parquet(f"{SILVER_BATCH}/dim_products").cache()
df_sellers  = spark.read.parquet(f"{SILVER_BATCH}/dim_sellers").cache()

df_items = spark.read.parquet(f"{RAW_BATCH}/olist_order_items")

df_enriched = (df_items
    .join(broadcast(df_products), "product_id", "left")
    .join(broadcast(df_sellers),  "seller_id",  "left")
)

# ==============================================================================
# EJEMPLO: ESCRITURA GOLD CON DELTA LAKE (MERGE Batch + Streaming)
# ==============================================================================
from delta.tables import DeltaTable

# Primera escritura (crear la tabla Delta)
(df_gold_weather
    .coalesce(4)
    .write
    .format("delta")
    .partitionBy("year", "month")
    .mode("overwrite")
    .save(f"{GOLD}/gold_weather_impact")
)

# Merge posterior (streaming actualiza datos existentes)
delta_table = DeltaTable.forPath(spark, f"{GOLD}/gold_weather_impact")

(delta_table.alias("target")
    .merge(
        df_streaming_new.alias("source"),
        "target.order_id = source.order_id AND target.event_date = source.event_date"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# ==============================================================================
# EJEMPLO: OPTIMIZE DELTA (Compactación anti-Small Files)
# ==============================================================================
spark.sql(f"OPTIMIZE delta.`{GOLD}/gold_weather_impact`")
spark.sql(f"OPTIMIZE delta.`{GOLD}/gold_sales_by_category_time`")

# ==============================================================================
# EJEMPLO: STRUCTURED STREAMING (Kafka → Raw/Streaming)
# ==============================================================================
df_kafka = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather_events")
    .option("startingOffsets", "latest")
    .load()
)

# Parsear JSON y escribir como Parquet particionado por hora
from pyspark.sql import functions as F

df_parsed = (df_kafka
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(F.from_json("json_str", weather_schema).alias("data"))
    .select("data.*")
    .withColumn("year",  F.year("event_timestamp"))
    .withColumn("month", F.month("event_timestamp"))
    .withColumn("day",   F.dayofmonth("event_timestamp"))
    .withColumn("hour",  F.hour("event_timestamp"))
)

(df_parsed.writeStream
    .format("parquet")
    .partitionBy("year", "month", "day", "hour")    # Granularidad horaria
    .option("checkpointLocation", f"{BUCKET}/_checkpoints/weather_raw")
    .option("path", f"{RAW_STREAMING}/weather_events")
    .trigger(processingTime="5 minutes")             # Micro-batch cada 5 min
    .start()
)
```

---

## 🔄 Estrategia de Ciclo de Vida (Lifecycle Management)

**🔵 Organizador:** Para mantener costos dentro del Free Tier, propongo las siguientes reglas de lifecycle en el bucket:

| Prefijo | Acción | Después de | Destino |
|---------|--------|------------|---------|
| `raw/batch/` | Transition | 90 días | S3 Glacier Instant Retrieval |
| `raw/streaming/` | Transition | 30 días | S3 Infrequent Access (IA) |
| `raw/streaming/` | Expiration | 365 días | Eliminación automática |
| `processed/` | Ninguna | — | Se mantiene en S3 Standard |
| `gold/` | Ninguna | — | Se mantiene en S3 Standard |

> **Justificación:** Los datos Raw son la fuente de la verdad, pero una vez procesados, rara vez se leen. Moverlos a Glacier después de 3 meses ahorra significativamente. Los eventos de streaming crudos son efímeros y pueden expirar después de 1 año. Las capas Silver y Gold son las que se consultan activamente y deben permanecer en Standard.

```json
{
  "Rules": [
    {
      "ID": "raw-batch-to-glacier",
      "Filter": { "Prefix": "raw/batch/" },
      "Status": "Enabled",
      "Transitions": [
        { "Days": 90, "StorageClass": "GLACIER_IR" }
      ]
    },
    {
      "ID": "raw-streaming-to-ia",
      "Filter": { "Prefix": "raw/streaming/" },
      "Status": "Enabled",
      "Transitions": [
        { "Days": 30, "StorageClass": "STANDARD_IA" }
      ],
      "Expiration": { "Days": 365 }
    }
  ]
}
```

---

## ✅ Consenso Final del Equipo

**🔵 Organizador:** La estructura Medallion con separación batch/streaming está clara. Cinco prefijos raíz, sin ambigüedad.

**🟢 Optimizador:** El particionamiento está controlado:
- Facts por su filtro de negocio más frecuente (tiempo o tipo de pago).
- Dimensiones sin partición (broadcast join).
- Geolocation como excepción por volumen.
- Streaming por hora (no minuto) para evitar Small Files.
- `coalesce(4)` en cada escritura para controlar el número de archivos.

**🔴 Eficiente:** Los formatos están definidos:
- Parquet + Snappy en Raw y Silver.
- Delta Lake + Snappy en Gold (ACID para merge batch+streaming).
- `OPTIMIZE` como tarea de mantenimiento en el DAG.

> **🤝 Los tres agentes acuerdan:** Este diseño escala sin superar el Free Tier, responde queries en segundos con Athena/Spark gracias al partition pruning y predicate pushdown, y maneja la convergencia batch-streaming sin conflictos gracias a Delta Lake.
