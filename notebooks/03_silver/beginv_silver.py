from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Upsert Sales Bronze to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# cargamos la delta y la abrimos con vista temporal
bronze_path = "./datalake/bronze/BegInv"
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("BegInv")

# limpiamos los datos
#normalizamos description y city
#sacamos las filas con nulos porque no sirven para el analisis
df_cleaned = spark.sql("""
SELECT 
  lower(trim(Description)) AS description,
  lower(trim(regexp_replace(City, '[^a-zA-Z0-9 ]', ''))) AS city,                     
  InventoryId,
  Brand,
  cast(StartDate as date) as StartDate
FROM BegInv
WHERE StartDate IS NOT NULL 
  AND description IS NOT NULL 
  AND InventoryId IS NOT NULL

""")

# Ruta de la tabla Silver
silver_path = "./datalake/silver/BegInv"

# Si no existe la tabla Silver, la creamos
from delta.tables import DeltaTable
import os
#hacemos un upsert
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
        AND target.StartDate = source.StartDate
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()