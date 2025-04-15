from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Exportar a PostgreSQL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# levantamos la tabla delta de gold
delta_table_path = "./datalake/gold/MarginsProfits"
df = spark.read.format("delta").load(delta_table_path)

# display para verificar lo que enviamos
df.show()

#configuración
jdbc_url = "jdbc:postgresql://shuttle.proxy.rlwy.net:19123/railway"
properties = {
    "user": "postgres",
    "password": "ItoEzlEowjgdjQaieWCSWakiBjmvsJmB",
    "driver": "org.postgresql.Driver"
}

#escritura en postgres
df.write \
    .jdbc(
        url=jdbc_url,
        table="margins_profits",
        mode="overwrite",  # o "append" si querés mantener lo anterior
        properties=properties
    )


