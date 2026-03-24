# Auditoría PostgreSQL — Supabase
**Proyecto:** PI_M4 Data Lake | **Fecha:** 2026-03-24 | **Bucket:** pi-m4-datalake-maxi

---

## 📋 Tablas Encontradas (9 tablas)

| Tabla | Filas |
|-------|------:|
| `olist_customers` | 99,441 |
| `olist_geolocation` | 2,000,326 |
| `olist_order_items` | 112,650 |
| `olist_order_payments` | 103,886 |
| `olist_order_reviews` | 99,224 |
| `olist_orders` | 99,441 |
| `olist_product_category_name_translation` | 71 |
| `olist_products` | 32,951 |
| `olist_sellers` | 3,095 |
| **TOTAL** | **~2,611,085** |

---

## 🗂️ Estructura de Columnas

### `olist_orders` (9 cols)
| Columna | Tipo | PK |
|---------|------|----|
| `order_id` | varchar(50) | ✅ |
| `customer_id` | varchar(50) | |
| `order_status` | varchar(30) | |
| `order_purchase_timestamp` | timestamp | |
| `order_approved_at` | timestamp | |
| `order_delivered_carrier_date` | timestamp | |
| `order_delivered_customer_date` | timestamp | |
| `order_estimated_delivery_date` | timestamp | |
| `updated_at` | timestamp | |

### `olist_order_items` (7 cols)
| Columna | Tipo | PK |
|---------|------|----|
| `order_id` | varchar(50) | ✅ |
| `order_item_id` | integer | ✅ |
| `product_id` | varchar(50) | |
| `seller_id` | varchar(50) | |
| `shipping_limit_date` | timestamp | |
| `price` | numeric | |
| `freight_value` | numeric | |

### `olist_order_payments` (6 cols)
| Columna | Tipo | PK |
|---------|------|----|
| `order_id` | varchar(50) | ✅ |
| `payment_sequential` | integer | ✅ |
| `payment_type` | varchar(30) | |
| `payment_installments` | integer | |
| `payment_value` | numeric | |
| `updated_at` | timestamp | |

### `olist_order_reviews` (8 cols)
| Columna | Tipo | PK |
|---------|------|----|
| `review_id` | varchar(50) | ✅ |
| `order_id` | varchar(50) | ✅ |
| `review_score` | integer | |
| `review_comment_title` | text | |
| `review_comment_message` | text | |
| `review_creation_date` | timestamp | |
| `review_answer_timestamp` | timestamp | |
| `updated_at` | timestamp | |

### `olist_products` (10 cols)
| Columna | Tipo | PK |
|---------|------|----|
| `product_id` | varchar(50) | ✅ |
| `product_category_name` | varchar(100) | |
| `product_name_length` | integer | |
| `product_description_length` | integer | |
| `product_photos_qty` | integer | |
| `product_weight_g` | integer | |
| `product_length_cm` | integer | |
| `product_height_cm` | integer | |
| `product_width_cm` | integer | |
| `updated_at` | timestamp | |

### `olist_customers` (5 cols)
`customer_id (PK)`, `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state`

### `olist_sellers` (5 cols)
`seller_id (PK)`, `seller_zip_code_prefix`, `seller_city`, `seller_state`, `updated_at`

### `olist_geolocation` (6 cols)
`geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng`, `geolocation_city`, `geolocation_state`, `updated_at`

### `olist_product_category_name_translation` (2 cols)
`product_category_name (PK)`, `product_category_name_english`

---

## 🔗 Relaciones (Foreign Keys)

```
olist_order_items.order_id   → olist_orders.order_id
olist_order_items.product_id → olist_products.product_id
olist_order_items.seller_id  → olist_sellers.seller_id
olist_order_payments.order_id → olist_orders.order_id
olist_order_reviews.order_id  → olist_orders.order_id
olist_orders.customer_id      → olist_customers.customer_id
```

### Diagrama E-R
```
olist_customers ──────────────────────────────────────────┐
       ↑ (customer_id)                                    │
  olist_orders ─────── olist_order_items ─── olist_products
       │                                │
       │                                └─── olist_sellers
       ├──── olist_order_payments
       └──── olist_order_reviews

olist_geolocation (standalone — join por zip_code_prefix implícito)
olist_product_category_name_translation (standalone — join por category_name)
```

> [!NOTE]
> `olist_geolocation` y `olist_product_category_name_translation` **no tienen FK formal** pero se relacionan implícitamente con `olist_customers`/`olist_sellers` (por zip) y `olist_products` (por category_name).

---

## ⚠️ Análisis de Nulls

| Tabla | Columna | Nulls | % | Criticidad |
|-------|---------|------:|---|-----------|
| `olist_products` | `product_name_length` | 32,951 | **100%** | 🔴 CRÍTICO |
| `olist_products` | `product_description_length` | 32,951 | **100%** | 🔴 CRÍTICO |
| `olist_order_reviews` | `review_comment_title` | 87,656 | **88.3%** | 🔴 CRÍTICO |
| `olist_order_reviews` | `review_comment_message` | 58,247 | **58.7%** | 🔴 CRÍTICO |
| `olist_products` | `product_category_name` | 610 | 1.9% | 🟡 LEVE |
| `olist_products` | `product_photos_qty` | 610 | 1.9% | 🟡 LEVE |
| `olist_orders` | `order_delivered_customer_date` | 2,965 | 3.0% | 🟡 LEVE |
| `olist_orders` | `order_delivered_carrier_date` | 1,783 | 1.8% | 🟡 LEVE |
| `olist_orders` | `order_approved_at` | 160 | 0.2% | 🟢 OK |

---

## 🛠️ Acciones Recomendadas en PySpark (Silver)

| Problema | Solución |
|----------|----------|
| `product_name_length` y `product_description_length` 100% null | Eliminar columnas en Silver (`drop`) |
| `review_comment_title/message` 88%/58% null | Rellenar con `""` o `"sin_comentario"` en Silver |
| `order_delivered_*` null | Son órdenes pendientes/canceladas — filtrar por `order_status = 'delivered'` |
| `product_category_name` null (610 filas) | Join con `category_translation` y fallback `"otros"` |
| `olist_geolocation` 2M registros con duplicados | Deduplicar por `zip_code_prefix` → ~20K únicos |
