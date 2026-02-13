class MinioSparkClient:

    def __init__(self, endpoint:str, access_key: str, secret_key:str, root_path: str = None, env_file = None):
        from pyspark.sql import SparkSession

        self._root_path = "s3a://"
        self._root_path += f"{root_path.strip("/")}/" if root_path else ""

        self._connected = False
        self._spark: SparkSession = None
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
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")


    def __read_file(self, _reader: callable, *paths, **options):
        return _reader(*[f"{self._root_path}{p.lstrip("/")}" for p in paths], **options)


    def __check_session(self):
        if not self._spark:
            raise RuntimeError("Spark session is not running. Use connect() to initiate it")


    def disconnect(self):
        if self._spark:
            self._spark.stop()
            self._connected = False


    def connect(self):
        if self._connected:
            self.disconnect()

        self._spark = self._spark_builder.getOrCreate()
        self._connected = True


    def read_parquet(self, *paths, **options):
        self.__check_session()
        return self.__read_file(self._spark.read.parquet, *paths, **options)

    
    def read_csv(self, *paths, **options):
        self.__check_session()
        return self.__read_file(self._spark.read.csv, *paths, **options)


    load_parquet    = read_parquet
    load_csv        = read_csv
