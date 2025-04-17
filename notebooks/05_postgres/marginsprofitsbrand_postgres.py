from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Exportar a PostgreSQL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# gold tables loading
delta_table_path = "./datalake/gold/MarginsProfitsBrand"
df = spark.read.format("delta").load(delta_table_path)

# display 
df.show()

#setting jdbc
jdbc_url = "jdbc:postgresql://shuttle.proxy.rlwy.net:19123/railway"
properties = {
    "user": "postgres",
    "password": "ItoEzlEowjgdjQaieWCSWakiBjmvsJmB",
    "driver": "org.postgresql.Driver"
}

#  postgresql writting
df.write \
    .jdbc(
        url=jdbc_url,
        table="margins_profits_brand",
        mode="overwrite",  
        properties=properties
    )
