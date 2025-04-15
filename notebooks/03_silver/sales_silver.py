from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Upsert Sales Bronze to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# cargamos la delta y la abrimos con vista temporal
bronze_path = "./datalake/bronze/Sales"
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("Sales")
# limpiamos los datos
#normalizamos description y city
#sacamos las filas con nulos porque no sirven para el analisis
df_cleaned = spark.sql("""
SELECT 
  lower(trim(Description)) AS description,
  lower(trim(VendorName)) AS vendorname,
  InventoryId,
  Brand,
  cast(SalesDollars as double) as SalesDollars,
  cast(SalesQuantity as double) as SalesQuantity,
  cast(SalesPrice as double) as SalesPrice,
  cast(SalesDate as date) as SalesDate,
  cast(ExciseTax as double) as ExciseTax
FROM Sales
WHERE SalesPrice IS NOT NULL 
  AND SalesQuantity IS NOT NULL 
  AND Description IS NOT NULL

""")

# Ruta de la tabla Silver
silver_path = "./datalake/silver/Sales"

from delta.tables import DeltaTable
import os

os.makedirs(silver_path, exist_ok=True)

#overwrite
df_cleaned.write.format("delta").mode("overwrite").save(silver_path)
