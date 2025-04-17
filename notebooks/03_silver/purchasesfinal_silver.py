from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Upsert Sales Bronze to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Loading delta table
bronze_path = "./datalake/bronze/PurchasesFINAL"
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("PurchasesFINAL")

#  Data cleansing
#normalize description y city
#let's discard rows with null in key columns
df_cleaned = spark.sql("""
SELECT 
  lower(trim(Description)) AS description,
  lower(trim(VendorName)) AS vendorname,
  InventoryId,
  cast(Brand as string) as brand,
  cast(Dollars as double) as PurchaseDollars,
  cast(Quantity as double) as PurchaseQuantity,
  cast(purchaseprice as double) as PurchasePrice,
  cast(Receivingdate as date) as PurchaseDate
FROM PurchasesFINAL
WHERE purchaseprice IS NOT NULL 
  AND quantity IS NOT NULL 
  AND Description IS NOT NULL

""")

# Silver path
silver_path = "./datalake/silver/PurchasesFINAL"

#If silver path doesnt exist lets creat it
from delta.tables import DeltaTable
import os
os.makedirs(silver_path, exist_ok=True)

#overwrite
df_cleaned.write.format("delta").mode("overwrite").save(silver_path)
