# Diseño de Arquitectura: Data Lake PIM4

**Autor:** Maximiliano Falco  
**Proyecto:** PI M4 - Data Engineering Bootcamp

---

## 1. Resumen Ejecutivo
El proyecto PI M4 despliega un pipeline escalable de extremo a extremo que implementa una **Arquitectura Lambda** moderna sobre la nube de AWS. La solución integra procesamiento Batch y Streaming, permitiendo a los analistas consumir información tanto histórica como en tiempo real.
El eje central del sistema recae en la ingesta, conformación y disponibilización de un Data Lakehouse que sigue el patrón de diseño Medallion (Raw, Silver/Processed, Gold) y soporta transacciones ACID utilizando **Delta Lake**.

---

## 2. Diagrama de Arquitectura

El siguiente diagrama detalla la ruta del dato desde los sistemas operacionales hasta el modelo analítico consolidado:

```mermaid
flowchart TD
    %% Fuentes de Datos
    subgraph Sources ["Sistemas Origen"]
        PG[(PostgreSQL Olist)]
        API[API Pública]
        APP((Eventos de Aplicación\nApp Olist))
    end

    %% Capa de Ingesta (Extract)
    subgraph Ingestion ["Capa de Ingesta / Landing"]
        AIR[Airbyte Cloud]
        KAFKA{Apache Kafka\nEn EC2}
        KAFKA_APP{Spark Streaming\nConsumer}
    end

    %% Storage: AWS S3 Data Lake
    subgraph DataLake ["AWS S3 Data Lake"]
        RAW_B[Raw Batch]
        RAW_S[Raw Streaming]
        SIL_B[Processed Batch / Silver\n(Delta Lake)]
        SIL_S[Processed Streaming\n(Parquet)]
        GOLD[Gold / Analytical\n(Delta Lake)]
    end

    %% Procesamiento & Transformación
    subgraph Processing ["Procesamiento / AWS Glue (Spark)"]
        JOB1(Spark Raw to Silver\nSCD Type 2)
        JOB2(Spark Speed Layer\nEnrichment)
        JOB3(Spark Silver to Gold\nLambda Unification)
        JOB4(Spark DQ Audit)
    end

    %% Orquestación & Gobernanza
    subgraph Governance ["Orquestación y Gobernanza"]
        AIRFLOW[[Apache Airflow en EC2]]
        LAKE[AWS Lake Formation]
        GLUE_CAT[(Glue Data Catalog)]
        ATHENA[Amazon Athena]
    end

    %% Flujos Batch
    PG --> AIR
    API --> AIR
    AIR -- Batch --> RAW_B
    RAW_B --> JOB1
    JOB1 -- Limpieza y SCD2 --> SIL_B
    SIL_B --> JOB3

    %% Flujos Streaming
    APP --> KAFKA
    KAFKA --> KAFKA_APP
    KAFKA_APP -- Eventos Vivo --> RAW_S
    RAW_S --> JOB2
    SIL_B -->|Left Join| JOB2
    JOB2 -- Enriquecido --> SIL_S
    SIL_S --> JOB3

    %% Capa Analítica
    JOB3 -- OBT y Agrupaciones --> GOLD
    GOLD --> GLUE_CAT
    GLUE_CAT --> ATHENA
    LAKE -. Permisos .-> GLUE_CAT

    %% DQ y Mantenimiento
    JOB1 -. Audita .-> JOB4

    %% Orquestación
    AIRFLOW -. Dispara y Monitorea .-> AIR
    AIRFLOW -. Dispara y Monitorea .-> JOB1
    AIRFLOW -. Dispara y Monitorea .-> JOB2
    AIRFLOW -. Dispara y Monitorea .-> JOB3

    %% Estilos
    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:white;
    classDef db fill:#336791,stroke:#232F3E,color:white;
    classDef s3 fill:#f44b42,stroke:#111,color:white;
    classDef process fill:#E1F5FE,stroke:#01579B,stroke-width:2px;
    
    class GOLD,SIL_B,RAW_B,RAW_S,SIL_S s3;
    class AIRFLOW,AIR aws;
    class ATHENA,LAKE,GLUE_CAT aws;
    class JOB1,JOB2,JOB3,JOB4,KAFKA_APP process;
```

---

## 3. Topología de Capas (Medallion Architecture)

Para asegurar el buen gobierno del dato, el bucket principal `pi-m4-datalake-maxi` está dividido formalmente por nivel de refinamiento:

### 3.1. Arquitectura Batch (Slow Lane)
1. **Raw (Bronce):** Aterrizaje del JSON o CSV puro en su formato nativo mediante conectores de *Airbyte*. Contiene el estado inmutable desde la base PostgreSQL (ej. Clientes, Órdenes, Pagos).
2. **Processed (Silver):** Los datos sufren limpieza, casting de tipos y des-anidado. Aquí aplico el formato **Delta Lake**, permitiéndonos gestionar Historial de Cambios Lentos (**SCD Tipo 2**) —específicamente en la tabla de `dim_customers`— para analizar cambios geográficos sin romper integridad de métricas previas.
3. **Gold:** Se construyen *One Big Tables* (OBT) orientadas al dominio de las preguntas de negocio. Ej. `gold_sales_by_category_time`, `gold_customer_segmentation` (RFM).

### 3.2. Arquitectura Streaming (Speed Layer)
1. **Raw-Streaming:** Los eventos fluyen desde la aplicación hacia un *Apache Kafka* desplegado en EC2. Un consumidor Spark 24/7 (`kafka_to_raw_streaming.py`) graba los lotes directamente en S3 evitando estresar la red empresarial.
2. **Processed-Streaming:** Mediante un micro-batching desencadenado por Airflow, tomamos la capa cruda, realizamos validación contra un Esquema Central, y hacemos un "Left Join" en vivo contra la dimensión estática (Silver) `dim_customers` para enriquecer el dato al vuelo.
3. **Unificación Lambda:** En la capa Gold (`silver_to_gold.py`), el Data Frame de Batch y el de Streaming concurren simultáneamente, eliminando duplicados mediante el id único (`order_id`) dando robustez analítica de tiempo real frente a las tardanzas del batch.

---

## 4. Stack Tecnológico (Selección y Justificación)

*   **Procesamiento (Apache Spark 4.0 / AWS Glue):** Seleccionado debido a su capacidad de cálculo en memoria, manejo de *Shuffles* controlados y compatibilidad nativa para integrar Delta Lake (ACID). Usar Glue Serverless abarata enormemente los costos operativos al encender el clúster únicamente bajo demanda orquestada.
*   **Gestión Delta Lake:** Permite usar sentencias `MERGE INTO`, Time Travel y resolver el gran problema de los *Data Swamps* al permitir transacciones seguras concurrentes frente a lecturas humanas en Athena.
*   **Orquestación (Apache Airflow y EC2):** Se utiliza sobre una instancia de cálculo constante dado que su scheduler requiere monitoreo persistente de dependencias (`task >> task >> task`). Resulta clave para poder inyectar alertas fallidas del flujo (ver integración de Slack configurada vía callbacks manuales en el DAG).
*   **Ingesta (Airbyte Cloud vs Kafka):** Se escogió usar **Airbyte** vía su API REST (Client Credentials) para la extracción programada (Batch) diaria por su altísima capacidad de auto-descubrimiento en bases RDBMS. Se complementa con **Kafka** (Streaming) para lidiar eficientemente con el back-pressure originado por los eventos de IoT o aplicaciones intensivas, funcionando como amortiguador.
*   **Gestión de Permisos (AWS Lake Formation):** Para otorgar gobernanza y seguridad. Se lo emplea inyectando credenciales exclusivas (`LakeformationCredentialsProvider`) a Spark, el cual delega si un rol externo puede alterar los catálogos en base a políticas de base de datos controladas por interfaz gráfica de AWS.

---

## 5. Casos de Negocio

El modelo de datos y las transformaciones implementadas están directamente alineadas a los interrogantes del negocio:
1. **Ventas por Categoría:** Visualización temporal de volúmenes en `gold_sales_by_category_time`.
2. **Segmentación RFM:** Modelo analítico agrupando hábitos transaccionales en `gold_customer_segmentation`.
3. **Métricas Estacionarias:** Obtención rápida del total facturado y el ticket promedio filtrados en OBT independientes.

---
_Nota: Validar dependencias con el archivo `deploy_dags.yml` (CI/CD GitHub Actions) y el artefacto .zip empaquetado para AWS Glue (`package_glue_lib.ps1`)._
