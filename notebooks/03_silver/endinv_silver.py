from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Upsert Sales Bronze to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# cargamos la delta y la abrimos con vista temporal
bronze_path = "./datalake/bronze/EndInv"
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("EndInv")

# limpiamos los datos
#normalizamos description y city
#sacamos las filas con nulos porque no sirven para el analisis
df_cleaned = spark.sql("""
SELECT 
  lower(trim(Description)) AS description,
  lower(trim(City)) AS city,                     
  InventoryId,
  Brand,
  cast(EndDate as date) as endDate
FROM EndInv
WHERE EndDate IS NOT NULL 
  AND description IS NOT NULL 
  AND InventoryId IS NOT NULL

""")

# Ruta de la tabla Silver
silver_path = "./datalake/silver/EndInv"

# Si no existe la tabla Silver, la creamos
from delta.tables import DeltaTable
import os

if not os.path.exists(silver_path):
    df_cleaned.write.format("delta").mode("overwrite").save(silver_path)
else:
    silver_table = DeltaTable.forPath(spark, silver_path)
    silver_table.alias("target").merge(
        df_cleaned.alias("source"),
        """
        target.description = source.description
        AND target.city = source.city
        AND target.InventoryId = source.InventoryId
        AND target.endDate = source.EndDate
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()