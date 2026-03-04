# Documentación del Pipeline de Limpieza de Outliers
Este script implementa la estrategia del _clip_, que consiste en establecer unos márgenes mínimo y máximo de tal manera que cualquier valor fuera del rango definido tome tome el valor del límite más cercano.
Esto solo es aplicable a las columnas de naturaleza numérica, claro está.

## Rangos de valores
- **Fare amount**:
    - Mínimo: 0
    - Máximo: 10000
- **Tip amount**:
    - Mínimo: 0
    - Máximo: 10000
- **Tolls amount**:
    - Mínimo: 0
    - Máximo: 5000
- **Total amount**:
    - Mínimo: 0
    - Máximo: 10000
- **Trip distance**:
    - Mínimo: 0
    - Máximo: 321868
