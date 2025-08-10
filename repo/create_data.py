MINIO_ACCESS_KEY = "qN0HFdwxcgEFMMh5Dkly"
MINIO_SECRET_KEY = "QX4F4BF38TbFPYpt9Mbv2Dhk3iIQ6SyEEjgS1B0q"
MINIO_ENDPOINT = "http://datalakehouse-minio.default.svc.cluster.local:9000"

jars = ",".join([
    "/home/jovyan/work/jars/hadoop-aws-3.3.4.jar",
    "/home/jovyan/work/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/home/jovyan/work/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar"
])


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, concat_ws, lower

def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spark-minio")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.jars", jars)
        .getOrCreate()
    )
       
    # Statik veri listesi (isim, soyisim, email)
    data = [
        ("Ahmet", "Yılmaz", "ahmet.yilmaz@gmail.com"),
        ("Ayşe", "Kaya", "ayse.kaya@yahoo.com"),
        ("Mehmet", "Demir", "mehmet.demir@hotmail.com"),
        ("Fatma", "Çelik", "fatma.celik@gmail.com"),
        ("Buraj", "Şahin", "Buraj.sahin@yahoo.com")
    ]
    
    # Kolon isimleri
    columns = ["first_name", "last_name", "email"]
    
    # DataFrame oluştur
    df = spark.createDataFrame(data, schema=columns)
    
    print("DF done")
    df.coalesce(1).write \
        .mode("overwrite") \
        .parquet("s3a://raw/data/data.parquet")
    
    spark.stop()


if __name__ == "__main__":
    main()