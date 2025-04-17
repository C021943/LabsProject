from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Upsert Sales Bronze to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Let's load delta table and open as a temporal view
bronze_path = "./datalake/bronze/BegInv"
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("BegInv")

# Data cleansing
#normalize description and city
#Let's discard rows with nulls in key columns
df_cleaned = spark.sql("""
SELECT 
  lower(trim(Description)) AS description,
  lower(trim(regexp_replace(City, '[^a-zA-Z0-9 ]', ''))) AS city,                     
  InventoryId,
  onHnad,
  Brand,
  cast(StartDate as date) as StartDate
FROM BegInv
WHERE StartDate IS NOT NULL 
  AND description IS NOT NULL 
  AND InventoryId IS NOT NULL

""")

#  Silver path
silver_path = "./datalake/silver/BegInv"

# If it doesn't existe, lets creat it
from delta.tables import DeltaTable
import os
#upsert
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
