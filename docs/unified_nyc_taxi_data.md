# Documentación de Datos de Registros de Viaje de NYC TLC

Este documento describe el esquema y la lógica de los tres conjuntos de datos principales proporcionados por la Comisión de Taxis y Limusinas de la Ciudad de Nueva York (NYC TLC) tras aplicar las transformaciones a las columnas correspondientes. Esto resulta en un mismo esquema unificado para cada conjunto de datos.

---

## Esquema de Datos
| Nombre de Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `VendorID` | Int8 | Código del proveedor de tecnología TPEP. |
| `pickup_datetime` | DateTime | Fecha y hora exacta en que se activó el taxímetro. |
| `dropoff_datetime` | DateTime | Fecha y hora exacta en que se desactivó el taxímetro. |
| `trip_distance` | Int32 | Distancia del viaje transcurrida reportada por el taxímetro (en metros). |
| `PULocationID` | Int16 | ID de la Zona de Taxi TLC donde comenzó el viaje. |
| `DOLocationID` | Int16 | ID de la Zona de Taxi TLC donde terminó el viaje. |
| `payment_type` | Int8 | Indica cómo pagó el pasajero. |
| `fare_amount` | Int16 | La tarifa de tiempo y distancia calculada por el medidor (en centavos). |
| `tip_amount` | Int16 | Cantidad pagada de propina (solo tarjetas de crédito, y en centavos). |
| `tolls_amount` | Int16 | Importe total de todos los peajes pagados (en centavos). |
| `total_amount` | Int16 | Suma total cobrada (en centavos). No incluye propinas en efectivo. |

## Notas Técnicas y Diccionarios

**VendorID:**
* **0:** Yellow taxi
* **1:** Green taxi
* **2:** Uber
* **3:** Lyft
* **4:** Other

**payment_type:**
* **0:** Flex fare tip
* **1:** Credit card
* **2:** Cash
* **3:** No charge
* **4:** Dispute
* **5:** Unknown
* **6:** Voided
* **7:** App
