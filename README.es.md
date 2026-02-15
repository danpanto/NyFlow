# NyFlow
## __Minimum deadheading, Maximum flow__

[English](README.md) | [Español](README.es.md)

### Developers:
- Daniel Pantoja Joaristi
- Jorge Hernández Palop
- Marco Casteleiro Trigueros
- Álvaro Arbona Rodríguez
- Carlos Jabalquinto Hernán-Gómez


# Descripción del proyecto
NyFlow es un sistema de optimización del flujo de tráfico diseñado para minimizar el deadheading (kilómetros recorridos por vehículos vacíos) mientras se maximiza el rendimiento general de la red. El proyecto incluye modelos de Deep Learning para predecir la demanda de vehículos y el precio en un futuro cercano.

## Características
- Análisis del flujo de tráfico en tiempo real
- Algoritmos inteligentes de enrutamiento de vehículos
- Estrategias de minimización de deadheading
- Optimización del rendimiento de la red
- Ajuste dinámico de rutas

## Installation
```bash
git clone <repository-url>
cd C-ity-enjoyers
```

## Uso

### Uso de UV
Este proyecto usa uv. Una vez instalado escribe en tu terminal el siguiente comando para sincronizar todo.
```bash
uv sync
```

### Visualization Tool
Para usar la herramienta de visualización localizada en la carpeta `visualitation`, ejecuta la siguiente línea:
```bash
uv run -m visualization.app.main
```
Una vez ejecutada se te proporcionará una url local donde se mostrará la interfaz gráfica.

#### Note

Algunas capas permiten hacer selección múltiple si pulsas CTRL mientras seleccionas la zona.

### Pipeline 
para usar el pipeline de descarga de  `pipeline` ejecuta la siguiente línea:
```bash
uv run -m  pipeline.run
```
Mostrará una interfaz interactiva que te ayudará a descargarte todos los datos que quieras.

### MinIO Credentials
Para ejecutar todo asegurate de tener las credenciales de MinIO:
```bash
export MINIO_ACCESS_KEY='your_access_key'
export MINIO_SECRET_KEY='your_secret_key'
export MINIO_ENDPOINT='your_endpoint'
```
Si no quieres usar variables de entorno algunos scripts funcionarán con un fichero local con estos datos.

Sustituye `'your_access_key'`,`'your_endpoint'`  y `'your_secret_key'` con las credenciales de MinIO.


## Información de los códigos y notebooks

El repositorio contiene varios cuadernos de Jupyter que proporcionan explicaciones detalladas de los componentes del proyecto. Cabe destacar que los cuadernos ubicados en el directorio models están minuciosamente documentados, incluyendo explicaciones de los algoritmos utilizados y las metodologías aplicadas. Cada cuaderno comienza con una lista de dependencias necesarias para ejecutar el código, asegurando que los usuarios puedan configurar su entorno fácilmente.

Además, los cuadernos sirven como guía para entender el procesamiento de datos, el entrenamiento de modelos y los procesos de evaluación, facilitando que los desarrolladores puedan contribuir al proyecto o adaptar los modelos para sus propios casos de uso.

## Tecnologías
En este proyecto principalmente usamos las siguientes tecnologías:

- **PyTorch**: Para entrenar los modelos de deep learning.
- **Polars**: Usado para una manipulación y análisis eficiente de los datos.
- **Apache Spark**: Usado para el tratamiento masivo de los datos.
- **MinIO**: Como herramienta de alamcenamiento.

Para un listado completo de todas las librearías se puede consultar el documento toml junto al resto de archivos de uv.

## License

Todos los modelos provienen de `'pytorch_forecasting'`.
Todos los datos han sido extraidos principalmente de `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`

Algunos notebooks vienen con referncias, recomendamos leerlas para entender en profundidad todo el proyecto.
