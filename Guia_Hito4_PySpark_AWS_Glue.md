# GUÍA DEFINITIVA: HITO 4 (PYSPARK EN AWS GLUE)
**Nivel:** Data Engineer Junior / Intermedio
**Objetivo:** Crear un entorno 100% Serverless en la Nube de AWS, sin usar credenciales estáticas (`.env`), procesando los datos de la capa Raw hacia Silver y Gold mediante Spark.

Elegiremos **AWS Glue** porque es el entorno más nativo y moderno para Spark en AWS. A diferencia de EMR (que requiere levantar servidores EC2) o Databricks (donde hay que exportar claves en la versión Community), Glue usa el concepto "Serverless" y se integra nativamente con roles IAM.

---

## FASE A: Preparación en AWS (IAM y S3)

Como Data Engineer Junior en la nube, la regla de oro es el **Least Privilege** (Mínimo Privilegio). Spark necesita permiso para leer y escribir en tu S3, pero NO a través de un usuario, sino a través de un **Rol**.

### Paso 1: Crear el Rol de IAM para AWS Glue
1. Entrá a la consola de AWS y buscá **IAM** (Identity and Access Management).
2. En el menú izquierdo, hacé clic en **Roles** > **Create role** (Crear rol).
3. En Entity type, seleccioná **AWS service**.
4. En Use case, elegí **Glue** y dale a Next.
5. Buscá y adjuntá estas dos políticas (Policies):
   - `AmazonS3FullAccess` (Si es entorno de pruebas, o una customizada solo para tu bucket `pi-m4-datalake-maxi`).
   - `AWSGlueServiceRole` (Imprescindible para que el servicio corra).
6. Dale nombre al rol: `Rol_Glue_DataLake` y finaliza la creación.

### Paso 2: Crear una carpeta para tus scripts en S3
Spark en AWS necesita que tu código en Python esté subido a S3.
1. Entrá al servicio **S3** y abrí tu bucket `pi-m4-datalake-maxi`.
2. Hacé clic en **Create folder** y llamalo `scripts`. (Ruta: `s3://pi-m4-datalake-maxi/scripts/`).

---

## FASE B: Creación del Código (Tu Trabajo como Developer)

Ahora sí, vas a crear dos archivos Python en tu compu local y luego los subiremos a esa carpeta. 

### Paso 3: Crear el script para la Capa Silver
Crea un archivo local que se llame `raw_to_silver.py`. Vas a notar que **NO CONFIGURAMOS CLAVES**. AWS Glue inyectará las credenciales del Rol que creaste en el Paso 1.

```python
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
# awsglue es la librería oficial de AWS que viene instalada en tu clúster invisible.
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import year, month

# 1. Inicialización 100% Nube
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# 2. Configuración de Performance (Soluciona el problema de "Small Files" de Athena)
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Rutas
bucket_in = "s3://pi-m4-datalake-maxi/raw/batch"
bucket_out = "s3://pi-m4-datalake-maxi/processed/batch"

print("Iniciando procesamiento a capa SILVER...")

# --------------------------------------------------------------------------
# DIM_GEOLOCATION (Deduplicación agresiva de millones a miles)
# --------------------------------------------------------------------------
df_geo = spark.read.parquet(f"{bucket_in}/olist_geolocation/")
df_geo = df_geo.dropDuplicates(['geolocation_zip_code_prefix'])
df_geo.write.mode("overwrite").partitionBy("geolocation_state").parquet(f"{bucket_out}/dim_geolocation/")

# --------------------------------------------------------------------------
# FACT_ORDERS (Particionamiento Temporal)
# --------------------------------------------------------------------------
df_orders = spark.read.parquet(f"{bucket_in}/olist_orders/")
df_orders = df_orders.withColumn("year", year("order_purchase_timestamp")) \
                     .withColumn("month", month("order_purchase_timestamp"))
df_orders.write.mode("overwrite").partitionBy("year", "month").parquet(f"{bucket_out}/fact_orders/")

# --------------------------------------------------------------------------
# FACT_PAYMENTS (Particionamiento Categórico)
# --------------------------------------------------------------------------
df_payments = spark.read.parquet(f"{bucket_in}/olist_order_payments/")
df_payments.write.mode("overwrite").partitionBy("payment_type").parquet(f"{bucket_out}/fact_payments/")

# TIPS PARA VOS: Abajo hacé lo mismo pero para customers, sellers, products, order_items y reviews!

job.commit()
print("¡Capa SILVER terminada exitosamente!")
```

### Paso 4: Crear el script para la Capa Gold
Crea localmente `silver_to_gold.py`. Este script leerá las cosas limpias, las cruzará y formará OBTs (*One Big Tables*). Presta atención a la función `broadcast()`, muy solicitada en entrevistas de Data Engineering.

```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import broadcast, sum as db_sum, count

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Solo leemos lo limpio de Silver!
bucket_silver = "s3://pi-m4-datalake-maxi/processed/batch"
bucket_gold = "s3://pi-m4-datalake-maxi/gold"

print("Iniciando procesamiento a capa GOLD...")

# --------------------------------------------------------------------------
# MODELO DE NEGOCIO: VENTAS POR CATEGORIA Y TIEMPO (OBT)
# --------------------------------------------------------------------------
# Leemos dos tablas Silver:
df_items = spark.read.parquet(f"{bucket_silver}/fact_order_items/")
df_products = spark.read.parquet(f"{bucket_silver}/dim_products/")

# El JOIN Maestro: usamos "broadcast" en products (tabla chica, ~33k filas).
# Esto copia 'products' a cada computadora del clúster de AWS y evita mover los 
# millones de 'items' por la red de AWS. ¡Es gratis, pero te hace ver súper Senior!
df_joined = df_items.join(broadcast(df_products), "product_id")

# Respondemos a la pregunta analítica:
df_gold_sales = df_joined.groupBy("product_category_name", "year", "month") \
                         .agg(
                             db_sum("price").alias("total_revenue"),
                             count("order_item_id").alias("total_items_sold")
                         )

# Guardamos la OBT particionada por año y mes para queries económicas en Athena
df_gold_sales.write.mode("overwrite").partitionBy("year", "month") \
             .parquet(f"{bucket_gold}/gold_sales_by_category_time/")

# TIPS PARA VOS: Continúa agregando los demás dfs Gold (Frecuencia, Región, Mmétodos de pago)

job.commit()
print("¡Capa GOLD terminada exitosamente!")
```

### Paso 5: Sube tus archivos
Agarrá esos dos `.py` y subilos manualmente (o por CLI) a tu bucket: `s3://pi-m4-datalake-maxi/scripts/`.

---

## FASE C: Ejecución en AWS Glue (Operaciones Cloud)

Ahora le decimos a AWS que agarre nuestro código de S3 y levante un clúster efímero para correrlo.

1. En la consola de AWS, buscá **AWS Glue**.
2. En el menú izquierdo ve a **ETL jobs**.
3. Seleccioná la opción: **Spark script editor** (Create a new script from scratch) y dale a **Create**.
4. En vez de tipear, andá a la pestaña **Job details**:
   - Name: `job_raw_to_silver`
   - IAM Role: Elegí `Rol_Glue_DataLake` (el que creamos al principio).
   - Tipo de Workers: G 1X (la versión más económica). Number of workers: 2 (el mínimo).
   - Advanced Properties -> **Script path**: Buscá haciendo clic en S3 tu script `s3://pi-m4-datalake-maxi/scripts/raw_to_silver.py`.
5. Dale al botón naranja arriba a la derecha: **Save** (Guardar).
6. Dale click a **Run**.

Ese botón "Run" levantará servidores ocultos (Serverless) por unos 2 o 3 minutos, bajará el código de S3, correrá tus transformaciones con su Rol IAM de forma extremadamente segura y eficiente, apagará los servidores, ¡y te cobrará centavos (o sacará horas del Free Tier)!

Hacé exactamente el mismo proceso de crear un Glue Job y darle RUN pero apuntando a `silver_to_gold.py`.

---

## FASE D: Orquestación con Apache Airflow (Consignas Fase 3)
*Nota conceptual para cuando configures tu EC2:*
Vos **NO** correrás los scripts mágicamente dándole Run en la consola todos los días. 
Airflow, desde tu EC2 (ubuntu), usará una librería (`GlueJobOperator`) en Python. Airflow solo grita la orden: *"¡Glue, prendéte y corre el job_raw_to_silver!"* y se queda esperando que termine.

**Asombroso, ¿no? ¡Pasaste de ejecutar algo local a armar una Pipeline de clase mundial en Cloud!**
