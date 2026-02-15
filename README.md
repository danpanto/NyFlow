# NyFlow
## __Minimum deadheading, Maximum flow__

[English](README.md) | [Español](README.es.md)

### Developers:
- Daniel Pantoja Joaristi
- Jorge Hernández Palop
- Marco Casteleiro Trigueros
- Álvaro Arbona Rodríguez
- Carlos Jabalquinto Hernán-Gómez


## Overview
NyFlow is a traffic flow optimization system designed to minimize deadheading (empty vehicle miles) while maximizing overall network throughput. The project includes Deep Learning models to predict the vehicles demand and prce in a nearby future.

## Features
- Real-time traffic flow analysis
- Intelligent vehicle routing algorithms
- Deadheading minimization strategies
- Network throughput optimization
- Dynamic route adjustment

## Installation
```bash
git clone <repository-url>
cd C-ity-enjoyers
```

## Usage

### Using UV
This project uses uv to create a virtual enviroment. To use it use install uv and write
```bash
uv sync
```

### Visualization Tool
To use the visualization tool located in the `visualitation` folder, run the following command:
```bash
uv run -m visualization.app.main
```
After running the command it will give you a local url which you can use to use the graphic interface.

### Pipeline Tool
To execute the pipeline in the `pipeline` folder, run:
```bash
uv run -m  pipeline.run
```
It will display an interactive app, follow all the steps to download all the files.

### MinIO Credentials
Ensure you have your MinIO credentials set in your environment variables:
```bash
export MINIO_ACCESS_KEY='your_access_key'
export MINIO_SECRET_KEY='your_secret_key'
export MINIO_ENDPOINT='your_endpoint'
```
If you don't want to use enviroment variables some scripts will ask you to create a document with does credentials.

Make sure to replace `'your_access_key'`,`'your_endpoint'`  and `'your_secret_key'` with your actual MinIO credentials.


## Scripts information

The repository contains several Jupyter notebooks that provide detailed explanations of the project's components. Notably, the notebooks located in the `models` directory are thoroughly documented, including explanations of the algorithms used and the methodologies applied. Each notebook begins with a list of dependencies required to run the code, ensuring that users can easily set up their environment.

Additionally, the notebooks serve as a guide for understanding the data processing, model training, and evaluation processes, making it easier for developers to contribute to the project or adapt the models for their own use cases.

## Technologies

This project utilizes several key technologies to enhance its functionality:

- **PyTorch**: Used for training deep learning models.
- **Polars**: Employed for efficient data manipulation and analysis.
- **Apache Spark**: Utilized for large-scale data processing.
- **MinIO**: A high-performance object storage solution for managing data.

For a complete list of libraries and dependencies, please refer to the corresponding toml documet

## License

All the models used comes from `'pytorch_forecasting'`.
All the data extracted come from `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`

Some notebooks have its own webgraphy. We recommend to read those articles which are helpful to understand the whole project.

<img src="https://img.shields.io/badge/GIT-black?style=for-the-badge&logo=GIT&logoColor=F05032"/>
<img src="https://img.shields.io/badge/PYTHON-black?style=for-the-badge&logo=python&logoColor=gold"/>
<img src="https://img.shields.io/badge/SH SCRIPTS-black?style=for-the-badge&logo=GNU Bash&logoColor=white"/>
<img src="https://img.shields.io/badge/numpy-black?style=for-the-badge&logo=numpy&logoColor=white"/>
<img src="https://img.shields.io/badge/JAVASCRIPT-black?style=for-the-badge&logo=JavaScript&logoColor=F7DF1E"/>
<img src="https://img.shields.io/badge/HTML-black?style=for-the-badge&logo=HTML5&logoColor=E34F26"/>
<img src="https://img.shields.io/badge/DJANGO-black?style=for-the-badge&logo=django&logoColor=white"/>
<img src="https://img.shields.io/badge/c-black?style=for-the-badge&logo=c&logoColor=A8B9CC"/>
<img src="https://img.shields.io/badge/c++-black?style=for-the-badge&logo=cplusplus&logoColor=00599C"/>
<img src="https://img.shields.io/badge/FASTAPI-black?style=for-the-badge&logo=fastapi&logoColor=009688"/>
<img src="https://img.shields.io/badge/FLASK-black?style=for-the-badge&logo=flask&logoColor=white"/>
<img src="https://img.shields.io/badge/CSS3-black?style=for-the-badge&logo=CSS3&logoColor=1572B6"/>
<img src="https://img.shields.io/badge/PYTORCH-black?style=for-the-badge&logo=pytorch&logoColor=EE4C2C"/>
<img src="https://img.shields.io/badge/pandas-black?style=for-the-badge&logo=pandas&logoColor=white"/>
<img src="https://img.shields.io/badge/TENSORFLOW-black?style=for-the-badge&logo=tensorflow&logoColor=FF6F00"/>
