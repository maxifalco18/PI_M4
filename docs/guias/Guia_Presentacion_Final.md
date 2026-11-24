# GUÍA DE DEFENSA TÉCNICA: PLATAFORMA DE DATOS PIM4 (Hitos 1-5)

**Objetivo:** Demostrar la robustez técnica, escalabilidad y observabilidad del proyecto a través de evidencias reales.

---

## 1. Caja 1: Infraestructura y Almacenamiento (Hito 1)
**Evidencia:** Consola AWS S3 y Lake Formation.

*   **Estructura Medallion**: Mostrá el bucket `pi-m4-datalake-maxi` y explicá la separación de responsabilidades:
    *   `/raw`: Inmutabilidad de los datos de origen.
    *   `/processed`: Datos limpios, tipados y listos para negocio.
    *   `/gold`: Tablas OBT para analítica.
*   **Gobernanza y Seguridad**: 
    *   **KMS**: Mostrá que el bucket está cifrado bajo demanda.
    *   **Lake Formation**: Explicá el registro del recurso para control de acceso centralizado.
*   **Verificación**: `aws s3 ls s3://pi-m4-datalake-maxi/`

---

## 2. Caja 2: Ingesta y Orquestación (Hito 2 y 3)
**Evidencia:** UI de Airflow y logs de Airbyte Cloud.

*   **Punto de Control (Airflow)**: Accedé a `http://100.31.44.224:8080`.
    *   **DAG `pi_m4_master_datalake_pipeline`**: Explicá el encadenamiento: Ingesta (Airbyte) -> Silver (Glue) -> Data Quality Audit -> Gold (Glue).
    *   **Variables de Entorno**: Mostrá en la pestaña *Admin > Variables* cómo parametrizamos el Bucket S3 para que el código sea portátil.
*   **Pipeline de Ingesta**: Explicá la integración con PostgreSQL (E-commerce) y OpenWeather API.

---

## 3. Caja 3: Procesamiento Batch (PySpark Medallion)
**Evidencia:** Código en `spark/` y Jobs de AWS Glue.

*   **Lógica Modular (`src/`)**: Abrí [`src/transformations/silver.py`](file:///c:/Users/CTI23994/Downloads/PIM4/src/transformations/silver.py).
    *   Explicá la normalización y el particionamiento temporal (`year/month`).
*   **Framework de Calidad**: Abrí [`src/components/dq/validator.py`](file:///c:/Users/CTI23994/Downloads/PIM4/src/components/dq/validator.py).
    *   Resaltá que no validamos a mano, sino con un framework extensible que revisa nulidad e integridad referencial (`expect_column_...`).
*   **Capa Gold**: Explicá las agregaciones clave (Ventas por región y categoría) en [`src/transformations/gold.py`](file:///c:/Users/CTI23994/Downloads/PIM4/src/transformations/gold.py).

---

## 4. Caja 4: Speed Layer y Unificación (Hito 5)
**Evidencia:** Producción de eventos en vivo y unificación Lambda.

*   **Kafka Producer Sim**: Ejecutá el simulador frente al jurado:
    ```bash
    python spark/kafka_producer_sim.py
    ```
*   **Unificación Lambda**: Abrí [`src/transformations/gold.py`](file:///c:/Users/CTI23994/Downloads/PIM4/src/transformations/gold.py) y buscá la función `unify_lambda_orders`.
    *   **El "Secret Sauce"**: Explicá cómo unimos el DataFrame de Spark Batch con el de Streaming (Kafka) eliminando duplicados por `order_id`.

---

## 5. El Cierre: Observabilidad y Certificación "Green"
**Evidencia:** Script `validate_demo.py`.

*   **Validación-como-Código**: Finalizá la demo corriendo:
    ```bash
    python validate_demo.py
    ```
*   **Qué significa esto**: Explicá que este script audita físicamente AWS y Airflow para garantizar que la plataforma está íntegra en ese instante preciso.

---

**¡Con esta estructura demostrás que tenés el control total de cada bit de información que fluye por la plataforma!**
