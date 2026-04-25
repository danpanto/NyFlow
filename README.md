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
NyFlow is a traffic flow optimization system designed to minimize deadheading (distance travelled by empty vehicles) while maximizing overall network throughput. The project includes Deep Learning models to predict vehicle demand and price in a nearby future.

## Features
- Real-time traffic flow analysis
- Smart vehicle routing algorithms
- Deadheading minimization strategies
- Network throughput optimization
- Dynamic route adjustment

## Installation
```bash
# Via HTTPS
git clone https://github.com/danpanto/NyFlow.git

# Via SSH
git clone git@github.com:danpanto/NyFlow.git

# Via GitHub's CLI
gh repo clone danpanto/NyFlow

cd C-ity-enjoyers
```

## Usage

### Using UV
This project uses uv for library management. Once installed, run the following command to sync everything:
```bash
uv sync
```

### Visualization Tool
To use the visualization tool located in the `visualitation` folder, run the following command:
```bash
uv run -m visualization.app.main
```
After running the command, you'll be given a local URL where the graphic interface will be displayed.

#### Note

Some layers allow multiple selections when holding down CTRL while selecting the zone.

### Pipeline Tool
To run the download pipeline (`pipeline` module), run the following command:
```bash
uv run -m pipeline.run
```
This will display an interactive interface, where you'll be able to download all the files you need, as well as preprocessing them.

### MinIO Credentials
Make sure to have your MinIO credentials available so as to run everything. We recommend having them as environment variables:
```bash
export MINIO_ACCESS_KEY='your_access_key'
export MINIO_SECRET_KEY='your_secret_key'
export MINIO_ENDPOINT='your_endpoint'
```
Replace `'your_access_key'`,`'your_endpoint'`  and `'your_secret_key'` with your actual MinIO credentials.

### Models training
Inside `models/predictor` directory you have 3 Jupyter notebooks to train the three models we mainly use: demand, amount and distance. Moreover, there is a python script with has a class to make global predictions (all variables) of the next 24 hours.

## Scripts information

The repository contains several Jupyter notebooks which provide detailed explanations of the project's components. Notably, the notebooks located in the `models` directory are thoroughly documented, including explanations of the algorithms used and the methodologies applied. Each notebook begins with a list of dependencies required to run the code, ensuring that users can easily set up their environment.

Additionally, the notebooks serve as a guide for understanding the data processing, model training, and evaluation processes, making it easier for developers to contribute to the project or adapt the models for their own use cases.

## Technologies

This project employs several key technologies to enhance its functionality:

- **PyTorch**: Used for training Deep Learning models.
- **Polars**: Efficient data manipulation and analysis.
- **Apache Spark**: Large-scale data processing.
- **MinIO**: A high-performance object storage solution for managing data.

For a complete list of libraries and dependencies, please refer to the corresponding TOML document and the rest of uv's files.

## License

All the models used come mainly from `'darts'`.
All the data extracted comes mainly from [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Some notebooks have its own webgraphy, which we recommend reading for a deeper understanding of the whole project.
