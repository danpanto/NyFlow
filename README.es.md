# NyFlow
## __Minimum deadheading, Maximum flow__

[English](README.md) | [Español](README.es.md)

### Desarrolladores:
- Daniel Pantoja Joaristi
- Jorge Hernández Palop
- Marco Casteleiro Trigueros
- Álvaro Arbona Rodríguez
- Carlos Jabalquinto Hernán-Gómez


# Descripción del proyecto
NyFlow es un sistema de optimización del flujo de tráfico diseñado para minimizar el deadheading (distancia recorrida por vehículos vacíos) mientras se maximiza el rendimiento general de la red. El proyecto incluye modelos de Deep Learning para predecir la demanda de vehículos y el precio en un futuro cercano.

## Características
- Análisis del flujo de tráfico en tiempo real
- Algoritmos inteligentes de enrutamiento de vehículos
- Estrategias de minimización de deadheading
- Optimización del rendimiento de la red
- Ajuste dinámico de rutas

## Instalación
```bash
# Mediante HTTPS
git clone https://github.com/danpanto/NyFlow.git

# Mediante SSH
git clone git@github.com:danpanto/NyFlow.git

# Mediante el CLI de GitHub
gh repo clone danpanto/NyFlow

cd C-ity-enjoyers
```

## Uso

### Uso de UV
Este proyecto utiliza uv para gestionar las librerías. Una vez instalado, ejecuta el siguiente comando para sincronizar todo:
```bash
uv sync
```

### Herramienta de visualización
Para usar la herramienta de visualización localizada en la carpeta `visualitation`, ejecuta la siguiente línea:
```bash
uv run -m visualization.app.main
```
Una vez ejecutada, se te proporcionará una URL local donde se mostrará la interfaz gráfica.

#### Nota

Algunas capas permiten hacer selección múltiple si pulsas CTRL mientras seleccionas la zona.

### Pipeline 
Para usar el pipeline de descarga (módulo `pipeline`), ejecuta la siguiente línea:
```bash
uv run -m pipeline.run
```
Mostrará una interfaz interactiva, desde donde podrás descargarte todos los datos que quieras, al igual que preprocesarlos.

### Credenciales de MinIO 
Para ejecutar todo, asegúrate de tener las credenciales disponibles. Recomendamos establecerlas como variables de entorno:
```bash
export MINIO_ACCESS_KEY='your_access_key'
export MINIO_SECRET_KEY='your_secret_key'
export MINIO_ENDPOINT='your_endpoint'
```
Sustituye `'your_access_key'`,`'your_endpoint'`  y `'your_secret_key'` con las credenciales de MinIO.

### Entrenamiento de modelos
Dentro del directorio `demand/predictor` hay 3 cuadernos de Jupyter para entrenar los tres modelos que usamos principalmente. Además, hay un código de python con una clase que sirve para hacer predicciones globales de las siguientes 24 horas.

## Información de los códigos y notebooks

El repositorio contiene varios cuadernos de Jupyter que proporcionan explicaciones detalladas de los componentes del proyecto. Cabe destacar que los cuadernos ubicados en el directorio `models` están minuciosamente documentados, incluyendo explicaciones de los algoritmos utilizados y las metodologías aplicadas. Cada cuaderno comienza con una lista de dependencias necesarias para ejecutar el código, asegurando que los usuarios puedan configurar su entorno fácilmente.

Además, los cuadernos sirven como guía para entender el procesamiento de datos, el entrenamiento de modelos y los procesos de evaluación, facilitando que los desarrolladores puedan contribuir al proyecto o adaptar los modelos para sus propios casos de uso.

## Tecnologías
En este proyecto usamos varias tecnologías clave para perfeccionar su funcionalidad:

- **PyTorch**: Para entrenar los modelos de Deep Learning.
- **Polars**: Para la manipulación y análisis eficiente de los datos en local.
- **Apache Spark**: Para el tratamiento masivo de los datos.
- **MinIO**: Como herramienta de alamcenamiento.

Para un listado completo de todas las librerías, se puede consultar el documento TOML junto al resto de archivos de uv.

## Licencias

Los modelos provienen principalmente de `'darts'`.

Los datos han sido extraidos principalmente de [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Algunos notebooks vienen con referencias, las cuales recomendamos leer para entender en profundidad todo el proyecto.
