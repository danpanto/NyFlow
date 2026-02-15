# Documentación de Datos de Registros de Viaje de NYC TLC

Este documento describe el esquema y la lógica de los tres conjuntos de datos principales proporcionados por la Comisión de Taxis y Limusinas de la Ciudad de Nueva York (NYC TLC) tras aplicar las transformaciones a las columnas correspondientes. Esto resulta en un mismo esquema unificado para cada conjunto de datos.

---

## Esquema de Datos
| Nombre de Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `VendorID` | Int8 | Código del proveedor de tecnología TPEP. |
| `pickup_datetime` | DateTime | Fecha y hora exacta en que se activó el taxímetro. |
| `dropoff_datetime` | DateTime | Fecha y hora exacta en que se desactivó el taxímetro. |
| `trip_distance` | Float32 | Distancia del viaje transcurrida en millas reportada por el taxímetro. |
| `PULocationID` | Int16 | ID de la Zona de Taxi TLC donde comenzó el viaje. |
| `DOLocationID` | Int16 | ID de la Zona de Taxi TLC donde terminó el viaje. |
| `payment_type` | String | Indica cómo pagó el pasajero. |
| `fare_amount` | Float32 | La tarifa de tiempo y distancia calculada por el medidor. |
| `tip_amount` | Float32 | Cantidad pagada de propina (solo tarjetas de crédito). |
| `tolls_amount` | Float32 | Importe total de todos los peajes pagados. |
| `total_amount` | Float32 | Suma total cobrada. No incluye propinas en efectivo. |
| `airport_fee` | Float32 | Tarifa de $1.25 por recogida en aeropuertos. |

## Notas Técnicas y Diccionarios

**VendorID:**
* **0:** Yellow taxi
* **1:** Green taxi
* **2:** Uber
* **3:** Lyft
* **4:** Other