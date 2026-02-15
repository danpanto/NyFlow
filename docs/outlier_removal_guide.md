# Documentación del Pipeline de Limpieza de Outliers
Este script implementa una estrategia de "Embudo Híbrido" (Hybrid Funneling) para la detección y eliminación de anomalías en grandes volúmenes de datos (Big Data).

## Resumen del Flujo de Trabajo
El proceso sigue una secuencia lógica de filtrado, de lo más simple y rápido a lo más complejo y computacionalmente costoso:

- Cálculo de Límites (DuckDB): Se calculan umbrales estadísticos globales.

- Filtrado Grueso (SQL): Se eliminan los valores imposibles o extremos usando Percentiles e IQR. Se genera un archivo intermedio.

- Filtrado Fino (Machine Learning): Se aplica Isolation Forest sobre los datos restantes para detectar anomalías contextuales.

## Métodos de Detección Utilizados
El script combina tres técnicas complementarias para asegurar una limpieza robusta:

1. Percentiles (Hard Capping):
Establece límites rígidos basados en la distribución de los datos. Corta las "colas" extremas.
    -  Inferior (0.1 / 10%): Elimina el 10% de los valores más bajos (se puede cambiar)

    - Superior (0.99 / 99%): Elimina el 1% de los valores más altos (se puede cambiar)

    Objetivo: Eliminar errores de medición obvios o valores absurdamente altos/bajos antes de pasar a cálculos más complejos.

2. Rango Intercuartílico (IQR):
Método estadístico robusto. Se basa en la diferencia entre el cuartil 75% (Q3) y el 25% (Q1). A mayor factor, más tolerancia a outliers.

    - Fusión con Percentiles: El script calcula ambos límites y elige el más restrictivo (Intersección), asegurando que los datos cumplan ambos criterios.

3. Isolation Forest:
Algoritmo no supervisado que aísla anomalías. A diferencia de los anteriores (que miran una columna a la vez), este analiza todas las columnas juntas.
Puede detectar, por ejemplo, un viaje que tiene una distancia corta pero un precio muy alto (algo que el IQR por separado no vería). El tamaño de la muestra de entrenamiento puede variar. La contaminación sirve para asumir el porcentaje de outliers que quedan.