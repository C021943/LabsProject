from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Upsert Sales Bronze to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# cargamos la delta y la abrimos con vista temporal
bronze_path = "./datalake/bronze/PurchasesFINAL"
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("PurchasesFINAL")

# limpiamos los datos
#normalizamos description y city
#sacamos las filas con nulos porque no sirven para el analisis
df_cleaned = spark.sql("""
SELECT 
  lower(trim(Description)) AS description,
  lower(trim(VendorName)) AS vendorname,
  InventoryId,
  Brand,
  cast(Dollars as double) as PurchaseDollars,
  cast(Quantity as double) as PurchaseQuantity,
  cast(purchaseprice as double) as PurchasePrice,
  cast(Receivingdate as date) as PurchaseDate
FROM PurchasesFINAL
WHERE purchaseprice IS NOT NULL 
  AND quantity IS NOT NULL 
  AND Description IS NOT NULL

""")

# Ruta de la tabla Silver
silver_path = "./datalake/silver/PurchasesFINAL"

# Si no existe la tabla Silver, la creamos
from delta.tables import DeltaTable
import os
os.makedirs(silver_path, exist_ok=True)

#overwrite
df_cleaned.write.format("delta").mode("overwrite").save(silver_path)
