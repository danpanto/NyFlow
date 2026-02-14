class MinioSparkClient:

    def __init__(self, endpoint:str, access_key: str, secret_key:str, root_path: str = None, env_file = None):
        from pyspark.sql import SparkSession
        

        self._root_path = "s3a://"
        self._root_path += f"{root_path.strip("/")}/" if root_path else ""
        self._connected = False

        self._spark: SparkSession = None
        self._FileSystem = None
        self._Path = None
        self._Conf = None
        
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
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
            .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g")


    def __check_session(self):
        if not self._spark:
            raise RuntimeError("Spark session is not running. Use connect() to initiate it")

    
    def __path(self, path: str):
        return f"{self._root_path}{path.strip("/")}"


    def __read_file(self, _reader: callable, file_format: str, path: str, **options):
        self.__check_session()
        return _reader.format(file_format).options(**options).load(self.__path(path))


    def __write_file(self, _writer: callable, file_format: str, path: str, **options):
        self.__check_session()
        _writer.format(file_format).options(**options).save(self.__path(path))


    def disconnect(self):
        if self._spark:
            self._spark.stop()
            self._spark: SparkSession = None
            self._FileSystem = None
            self._Path = None
            self._Conf = None
            self._connected = False


    def connect(self):
        if self._connected:
            self.disconnect()

        self._spark = self._spark_builder.getOrCreate()
        self._spark.sparkContext.setLogLevel("ERROR")
        self._FileSystem = self._spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem
        self._Path = self._spark.sparkContext._jvm.org.apache.hadoop.fs.Path
        self._Conf = self._spark.sparkContext._jsc.hadoopConfiguration()
        self._connected = True


    def rm(self, path: str):
        self.__check_session()

        full_path = self.__path(path)

        hadoop_path = self._Path(full_path)
        fs = self._FileSystem.get(hadoop_path.toUri(), self._Conf)

        if not fs.exists(hadoop_path):
            print(f"File '{full_path}' does not exist")
            return

        if fs.getFileStatus(hadoop_path).isDirectory():
            raise PermissionError(f"Refusing to delete '{full_path} (is a directory)'. Use rmdir() to delete it.")
        
        if fs.delete(hadoop_path):
            print(f"Successfully deleted: {full_path}")
        else:
            print(f"Failed to delete: {full_path}")


    def rmdir(self, path: str, force: bool = False):
        self.__check_session()

        full_path = self.__path(path)

        hadoop_path = self._Path(full_path)
        fs = self._FileSystem.get(hadoop_path.toUri(), self._Conf)

        if not fs.exists(hadoop_path):
            raise FileNotFoundError(f"Directory '{full_path}' does not exist")

        if not fs.getFileStatus(hadoop_path).isDirectory():
            raise PermissionError(f"Refusing to delete '{full_path}'. Use rm() to delete files.")

        if len(fs.listStatus(hadoop_path)) > 0 and not force:
            raise PermissionError(f"Refusing to delete '{full_path}' (directory is not empty). Use force=True.")
        
        if fs.delete(hadoop_path, True):
            print(f"Successfully deleted: {full_path}")
        else:
            print(f"Failed to delete: {full_path}")


    def read_parquet(self, path, **options):
        return self.__read_file(self._spark.read, "parquet", path, **options)

    
    def read_csv(self, path, **options):
        return self.__read_file(self._spark.read, "csv", path, **options)


    def write_parquet(self, data, path: str, **options):
        self.__write_file(data.write.mode("overwrite"), "parquet", path, **options)


    def write_csv(self, data, path: str, **options):
        self.__write_file(data.write.mode("overwrite"), "csv", path, **options)
