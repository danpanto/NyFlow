from pyspark.sql import SparkSession, DataFrameReader, DataFrameWriter
from minio import Minio


class MinioSparkClient:
    """
    Client for handling connection to MinIO cluster through PySpark API

    Attributes:
        self._connected     (bool):                 Whether the client is connected to the cluster
        self._spark         (SparkSession):         The PySpark session connected to MinIO
        self._spark_builder (SparkSession.Builder): The session builder, with all the configuration params
    """
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str = "pd2",
        base_dir: str = "cityenjoyer", memory: int = 2, heapsize: int = 2, num_part: int = 25):

        from pathlib import Path


        self._bucket = bucket_name.strip('/')
        self._base_dir = base_dir.strip('/')
        
        self._minio = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=True
        )
        
        self._connected: bool = False

        JARS_DIR = Path(__file__).parent.parent / "spark_jars"
        spark_jars = ",".join([
            str(JARS_DIR / "hadoop-aws-3.4.1.jar"),
            str(JARS_DIR / "bundle-2.24.6.jar"),
            str(JARS_DIR / "wildfly-openssl-1.1.3.Final.jar"),
        ])

        # Set up spark config
        self._spark_builder = SparkSession.builder \
            .appName("MinioSparkClient") \
            .config("spark.jars", spark_jars)

        # Set up MinIO credentials   
        self._spark_builder = self._spark_builder \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        
        # Configure MinIO-Spark connection
        self._spark_builder = self._spark_builder \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.socket.timeout", "600000") \
            .config("spark.hadoop.fs.s3a.paging.maximum", "1000") \
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
            .config("spark.hadoop.fs.s3a.multipart.size", "128M") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "600000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000") \
            .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
            
        # Configure spark resources
        self._spark_builder = self._spark_builder \
            .config("spark.storage.memoryFraction", "0.1") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.driver.memory", f"{memory}g") \
            .config("spark.executor.memory", f"{memory}g") \
            .config("spark.sql.shuffle.partitions", f"{num_part}") \
            .config("spark.memory.offHeap.size", f"{heapsize}g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.shuffle.compress", "true") \
            .config("spark.shuffle.spill.compress", "true") \

        # Vectorization config
        self._spark_builder = self._spark_builder \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.hadoop.fs.s3a.vectored.active", "false") \
            .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false") \


    def __check_session(self):
        if not self._connected:
            raise RuntimeError("Spark session is not running. Use connect() to initiate it")

    
    def __path(self, path: str):
        return f"s3a://{self._bucket}/{self._base_dir}/{path.strip("/")}"


    def __read_file(self, _reader: DataFrameReader, file_format: str, path, **options):
        self.__check_session()

        if isinstance(path, (list, set, tuple)):
            full_paths = [self.__path(p) for p in path]
        else:
            full_paths = [self.__path(path)]

        return _reader.format(file_format).options(**options).load(full_paths)


    def __write_file(self, _writer: DataFrameWriter, file_format: str, path: str, **options):
        self.__check_session()
        _writer.format(file_format).options(**options).save(self.__path(path))


    def disconnect(self):
        if self._connected:
            self._connected = False
            if self._spark:
                self._spark.stop()
                self._spark = None


    def connect(self):
        import os, sys

        if self._connected:
            return

        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        old_stderr_fd = os.dup(2)
        os.dup2(devnull_fd, 2)
        os.close(devnull_fd)

        try:
            self._spark: SparkSession | None = self._spark_builder.getOrCreate()
        finally:
            os.dup2(old_stderr_fd, 2)
            os.close(old_stderr_fd)

        self._spark.sparkContext.setLogLevel("ERROR")
        self._connected = True


    def dir_exists(self, dir_name):
        return any(
            True
            for _ in self._minio.list_objects(
                bucket_name=self._bucket,
                prefix=f"{self._base_dir}/{dir_name.strip('/')}/",
                recursive=False
            )
        )


    def mkdir(self, dir_name, exist_ok: bool = False):
        from io import BytesIO

        if not exist_ok and self.dir_exists(dir_name):
            return False

        self._minio.put_object(
            bucket_name=self._bucket,
            object_name=f"{self._base_dir}/{dir_name.strip('/')}/",
            data=BytesIO(b""),
            length=0
        )

        return True


    def list_objects(self, path: str, recursive: bool = False, **kwargs):
        return self._minio.list_objects(
            bucket_name=self._bucket,
            prefix=f"{self._base_dir}/{path.strip('/')}{'/' if path != "" else ""}",
            recursive=recursive,
            **kwargs
        )


    def read_parquet(self, path, **options):
        return self.__read_file(self._spark.read, "parquet", path, **options)  #type:ignore

    
    def read_csv(self, path, **options):
        return self.__read_file(self._spark.read, "csv", path, **options)  #type:ignore


    def write_parquet(self, data, path: str, **options):
        self.__write_file(data.write.mode("overwrite"), "parquet", path, **options)


    def write_csv(self, data, path: str, **options):
        self.__write_file(data.write.mode("overwrite"), "csv", path, **options)
