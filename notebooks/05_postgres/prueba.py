from pyspark.sql import SparkSession

# Inicializar SparkSession con Delta Lake
spark = SparkSession.builder \
    .appName("Exportar MarginsProfits a CSV") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Cargar la tabla Delta desde Gold
df = spark.read.format("delta").load("./datalake/gold/MarginsProfits")
df.show()
# Guardar como CSV en una nueva carpeta dentro de datalake/sigma
df.coalesce(1).write.mode("overwrite").option("header", "true").csv("./datalake/sigma/MarginsProfitsCSV")


