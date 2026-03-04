from pyspark.sql import SparkSession, DataFrameReader, DataFrameWriter
from py4j.java_gateway import JavaClass, JavaObject


class MinioSparkClient:
    """
    Client for handling connection to MinIO cluster through PySpark API

    Attributes:
        self._root_path     (str):                  Root path of all the files inside the cluster
        self._connected     (bool):                 Whether the client is connected to the cluster
        self._spark         (SparkSession):         The PySpark session connected to MinIO
        self._spark_builder (SparkSession.Builder): The session builder, with all the configuration params
        self._FileSystem    (JavaClass):            Haddop's own file system type
        self._Path          (JavaClass):            Haddop's own file path type
        self._Conf          (JavaObject):           Haddop's configuration in for the connection
    """
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, root_path: str = "",
        memory: int = 2, heapsize: int = 2, num_part: int = 25):
        
        self._root_path: str = "s3a://"
        self._root_path += f"{root_path.strip("/")}/"
        self._connected: bool = False
        self._spark_builder = SparkSession.builder \
            .appName("MinioSparkClient") \
            .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:3.4.1") \
            .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.vectored.active", "false") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "600000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000") \
            .config("spark.hadoop.fs.s3a.socket.timeout", "600000") \
            .config("spark.hadoop.fs.s3a.paging.maximum", "1000") \
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
            .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
            .config("spark.driver.memory", f"{memory}g") \
            .config("spark.executor.memory", f"{memory}g") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", f"{heapsize}g") \
            .config("spark.sql.shuffle.partitions", f"{num_part}") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.storage.memoryFraction", "0.1") \
            .config("spark.shuffle.compress", "true") \
            .config("spark.shuffle.spill.compress", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.multipart.size", "128M") \
            # .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")


    def __check_session(self):
        if not self._connected:
            raise RuntimeError("Spark session is not running. Use connect() to initiate it")

    
    def __path(self, path: str):
        return f"{self._root_path}{path.strip("/")}"


    def __read_file(self, _reader: DataFrameReader, file_format: str, path: str, **options):
        self.__check_session()
        return _reader.format(file_format).options(**options).load(self.__path(path))


    def __write_file(self, _writer: DataFrameWriter, file_format: str, path: str, **options):
        self.__check_session()
        _writer.format(file_format).options(**options).save(self.__path(path))


    def disconnect(self):
        self.__check_session()
        if self._spark:
            self._spark.stop()
            self._spark = None

        self._FileSystem = None
        self._Path = None
        self._Conf = None
        self._connected = False


    def connect(self):
        if self._connected:
            self.disconnect()

        self._spark: SparkSession | None = self._spark_builder.getOrCreate()
        self._spark.sparkContext.setLogLevel("ERROR")

        self._FileSystem:   JavaClass | None =  self._spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem   #type:ignore
        self._Path:         JavaClass | None =  self._spark.sparkContext._jvm.org.apache.hadoop.fs.Path         #type:ignore
        self._Conf:         JavaObject | None = self._spark.sparkContext._jsc.hadoopConfiguration()             #type:ignore
        self._connected = True


    def rm(self, path: str):
        self.__check_session()

        full_path: str = self.__path(path)

        hadoop_path: JavaObject = self._Path(full_path)  #type:ignore
        fs: JavaObject = self._FileSystem.get(hadoop_path.toUri(), self._Conf)  #type:ignore

        if not fs.exists(hadoop_path):  #type:ignore
            print(f"File '{full_path}' does not exist")
            return

        if fs.getFileStatus(hadoop_path).isDirectory():  #type:ignore
            raise PermissionError(f"Refusing to delete '{full_path} (is a directory)'. Use rmdir() to delete it.")
        
        if fs.delete(hadoop_path):  #type:ignore
            print(f"Successfully deleted: {full_path}")
        else:
            print(f"Failed to delete: {full_path}")


    def rmdir(self, path: str, force: bool = False):
        self.__check_session()

        full_path = self.__path(path)

        hadoop_path: JavaObject = self._Path(full_path)  #type:ignore
        fs: JavaObject = self._FileSystem.get(hadoop_path.toUri(), self._Conf)  #type:ignore

        if not fs.exists(hadoop_path):  #type:ignore
            raise FileNotFoundError(f"Directory '{full_path}' does not exist")

        if not fs.getFileStatus(hadoop_path).isDirectory():  #type:ignore
            raise PermissionError(f"Refusing to delete '{full_path}'. Use rm() to delete files.")

        if len(fs.listStatus(hadoop_path)) > 0 and not force:  #type:ignore
            raise PermissionError(f"Refusing to delete '{full_path}' (directory is not empty). Use force=True.")
        
        if fs.delete(hadoop_path, True):  #type:ignore
            print(f"Successfully deleted: {full_path}")
        else:
            print(f"Failed to delete: {full_path}")


    def read_parquet(self, path, **options):
        return self.__read_file(self._spark.read, "parquet", path, **options)  #type:ignore

    
    def read_csv(self, path, **options):
        return self.__read_file(self._spark.read, "csv", path, **options)  #type:ignore


    def write_parquet(self, data, path: str, **options):
        self.__write_file(data.write.mode("overwrite"), "parquet", path, **options)


    def write_csv(self, data, path: str, **options):
        self.__write_file(data.write.mode("overwrite"), "csv", path, **options)
