# Documentación de Datos de Registros de Viaje de NYC TLC

Este documento describe el esquema y la lógica de los tres conjuntos de datos principales proporcionados por la Comisión de Taxis y Limusinas de la Ciudad de Nueva York (NYC TLC).

---

## 1. Registros de Viajes de Taxis Amarillos (Yellow Taxi)

**Resumen:** Representa los taxis amarillos "tradicionales" de parada en la calle. Estos vehículos sirven principalmente a Manhattan y los aeropuertos (JFK/LGA). Los datos se recopilan a través del sistema TPEP (Taxicab Passenger Enhancement Program).

### Características Clave
* **Método:** Exclusivamente paradas en la calle (o paradas de taxi).
* **Precios:** Tarifa medida por taxímetro (Tiempo + Distancia) o Tarifa Plana (solo JFK).
* **Pagos:** Las propinas en efectivo **no** se registran (su valor es casi siempre 0).

### Esquema de Datos

| Nombre de Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `VendorID` | Entero | Código del proveedor de tecnología TPEP. |
| `tpep_pickup_datetime` | FechaHora | Fecha y hora exacta en que se activó el taxímetro. |
| `tpep_dropoff_datetime` | FechaHora | Fecha y hora exacta en que se desactivó el taxímetro. |
| `passenger_count` | Entero | Número de pasajeros (ingresado manualmente por el conductor). |
| `trip_distance` | Float | Distancia del viaje transcurrida en millas reportada por el taxímetro. |
| `RatecodeID` | Entero | Código de tarifa final vigente al final del viaje. |
| `store_and_fwd_flag` | Texto | Indica si el registro se mantuvo en memoria antes de enviarse. |
| `PULocationID` | Entero | ID de la Zona de Taxi TLC donde comenzó el viaje. |
| `DOLocationID` | Entero | ID de la Zona de Taxi TLC donde terminó el viaje. |
| `payment_type` | Entero | Código numérico que indica cómo pagó el pasajero. |
| `fare_amount` | Float | La tarifa de tiempo y distancia calculada por el medidor. |
| `extra` | Float | Extras y recargos varios (ej. hora pico). |
| `mta_tax` | Float | Impuesto MTA de $0.50 activado automáticamente. |
| `tip_amount` | Float | Monto de la propina (Solo tarjetas de crédito). |
| `tolls_amount` | Float | Importe total de todos los peajes pagados. |
| `improvement_surcharge` | Float | Recargo de $0.30 para el Fondo de Mejora de Taxis. |
| `total_amount` | Float | Suma total cobrada. No incluye propinas en efectivo. |
| `congestion_surcharge` | Float | Recargo por viajes al sur de la calle 96 en Manhattan. |
| `Airport_fee` | Float | Tarifa de $1.25 por recogida en aeropuertos. |
| `cbd_congestion_fee` | Float | Tarifa de peaje del Distrito Central de Negocios (si aplica, desde enero 2025). |

### Notas Técnicas y Diccionarios (Yellow Taxi)

**VendorID (Proveedor):**
* **1:** Creative Mobile Technologies, LLC
* **2:** Curb Mobility, LLC
* 6: Myle Technologies Inc
* 7: Helix

**RatecodeID (Código de Tarifa):**
* **1:** Tarifa Estándar (La mayoría de viajes urbanos)
* **2:** JFK (Tarifa Plana fija entre Manhattan y JFK)
* **3:** Newark (Tarifa medida + recargo)
* **4:** Nassau o Westchester (Tarifa doble al salir de la ciudad)
* **5:** Tarifa Negociada (Acordada fuera del taxímetro)
* **6:** Viaje Grupal
* 99: Nulo/Desconocido

**Payment_type (Forma de Pago):**
* **1:** Tarjeta de Crédito (Datos de propinas fiables)
* **2:** Efectivo (Cash - Datos de propinas nulos/vacíos/poco fiables)
* **3:** Sin Cargo (Viajes de prueba, policía, mecánicos)
* **4:** Disputa (El pasajero se negó a pagar)
* **5:** Desconocido
* **6:** Viaje Anulado

**Store_and_fwd_flag:**
* **Y:** Sí (El viaje se guardó localmente porque no había conexión y se envió después)
* **N:** No

---

## 2. Registros de Viajes de Taxis Verdes (Green Taxi)

**Resumen:** Representa los "Boro Taxis" (Taxis Verdes). Tienen permitido recoger pasajeros en los distritos exteriores (Queens, Brooklyn, Bronx) y el norte de Manhattan. Tienen prohibido recoger pasajeros en la "Zona Amarilla" de Manhattan (al sur de la calle 110 Oeste / 96 Este).

### Características Clave
* **Método:** Híbrido. Pueden funcionar como taxi de calle o como servicio de coche con chofer.
* **Precios:** Tarifa medida por taxímetro.

### Esquema de Datos

| Nombre de Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `VendorID` | Entero | Código del proveedor LPEP. |
| `lpep_pickup_datetime` | FechaHora | Hora de activación del taxímetro. |
| `lpep_dropoff_datetime` | FechaHora | Hora de desactivación del taxímetro. |
| `store_and_fwd_flag` | Texto | Bandera de almacenamiento en memoria. |
| `RatecodeID` | Entero | Código de tarifa (Ver sección Yellow Taxi). |
| `PULocationID` | Entero | ID de Zona de Recogida. |
| `DOLocationID` | Entero | ID de Zona de Destino. |
| `passenger_count` | Entero | Conteo de pasajeros. |
| `trip_distance` | Float | Millas recorridas. |
| `fare_amount` | Float | Tarifa calculada por el medidor. |
| `extra` | Float | Extras y recargos. |
| `mta_tax` | Float | Impuesto MTA. |
| `tip_amount` | Float | Monto de propina (Solo tarjeta). |
| `tolls_amount` | Float | Peajes pagados. |
| `ehail_fee` | Float | **Único:** Tarifa si se solicitó vía app. |
| `improvement_surcharge` | Float | Recargo de mejora. |
| `total_amount` | Float | Total cobrado. |
| `payment_type` | Entero | Forma de pago. |
| `trip_type` | Entero | **Único:** Indica cómo se inició el viaje. |
| `congestion_surcharge` | Float | Recargo por congestión. |
| `cbd_congestion_fee` | Float | Tarifa del Distrito Central de Negocios. |

### Notas Técnicas y Diccionarios (Green Taxi)
 

**Trip_type (Tipo de Viaje):**
* **1:** Street-hail (El pasajero levantó la mano en la calle).
* **2:** Dispatch (El pasajero llamó a una base para solicitar el viaje).

**Ehail_fee:**
* Esta columna suele estar vacía en datos recientes. Se usaba cuando Uber/Lyft intentaron integrar taxis verdes en sus apps.

---

## 3. Vehículos de Alquiler de Alto Volumen (HVFHV)

**Resumen:** Representa viajes basados en aplicaciones de empresas como **Uber y Lyft**.

### Características Clave
* **Método:** Preestablecido digitalmente.
* **Precios:** Dinámicos (Upfront pricing).
* **Análisis:** Permite calcular tiempos de espera (Request vs Pickup) y comisión de la plataforma (Driver Pay vs Base Fare).

### Esquema de Datos

| Nombre de Columna | Tipo | Descripción |
| :--- | :--- | :--- |
| `hvfhs_license_num` | Texto | Número de licencia de la empresa (App). |
| `dispatching_base_num` | Texto | La base específica que autoriza el viaje. |
| `originating_base_num` | Texto | La base que recibió la solicitud original. |
| `request_datetime` | FechaHora | Cuando el pasajero solicitó el viaje (Click en App). |
| `on_scene_datetime` | FechaHora | Cuando el conductor llegó al punto de recogida. |
| `pickup_datetime` | FechaHora | Cuando el viaje comenzó (Pasajero sube). |
| `dropoff_datetime` | FechaHora | Cuando terminó el viaje. |
| `PULocationID` | Entero | ID de Zona de Recogida. |
| `DOLocationID` | Entero | ID de Zona de Destino. |
| `trip_miles` | Float | Distancia recorrida. |
| `trip_time` | Entero | Duración del viaje en segundos. |
| `base_passenger_fare` | Float | Lo que pagó el pasajero (antes de impuestos/propinas). |
| `tolls` | Float | Peajes cobrados. |
| `bcf` | Float | Recargo del "Black Car Fund". |
| `sales_tax` | Float | Impuesto sobre ventas. |
| `congestion_surcharge` | Float | Recargo por zona de congestión. |
| `airport_fee` | Float | Tarifa por aeropuerto. |
| `tips` | Float | Propinas (en la app). |
| `driver_pay` | Float | Lo que ganó el conductor. |
| `shared_request_flag` | Texto | Si se solicitó viaje compartido. |
| `shared_match_flag` | Texto | Si realmente se compartió el viaje. |
| `access_a_ride_flag` | Texto | Si es un viaje de paratránsito (MTA). |
| `wav_request_flag` | Texto | Si se pidió vehículo accesible (Silla de ruedas). |
| `wav_match_flag` | Texto | Si llegó un vehículo accesible. |
| `cbd_congestion_fee` | Float | Tarifa del Distrito Central de Negocios. |

### Notas Técnicas y Diccionarios (HVFHV)

**Hvfhs_license_num (Identificador de App):**
* HV0002: Juno
* **HV0003:** Uber
* HV0004: Via
* **HV0005:** Lyft

**Cálculo de Tiempos:**
* **Tiempo de Espera del Pasajero:** `pickup_datetime` - `request_datetime`
* **Tiempo de Espera del Conductor (en sitio):** `pickup_datetime` - `on_scene_datetime`

**Banderas (Flags):**
* Todas las columnas que terminan en `_flag` usan **Y** (Yes/Sí) y **N** (No).
* **WAV:** Siglas de "Wheelchair Accessible Vehicle" (Vehículo Accesible para Silla de Ruedas).
