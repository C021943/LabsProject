from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Upsert Sales Bronze to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Let's load delta table and open as a temporal view
bronze_path = "./datalake/bronze/EndInv"
spark.read.format("delta").load(bronze_path).createOrReplaceTempView("EndInv")

# Data cleansing
#normalize description and city
#Let's discard rows with nulls in key columns
df_cleaned = spark.sql("""
SELECT 
  lower(trim(Description)) AS description,
  lower(trim(City)) AS city,                     
  InventoryId,
  onHand,
  Brand,
  cast(EndDate as date) as endDate
FROM EndInv
WHERE EndDate IS NOT NULL 
  AND description IS NOT NULL 
  AND InventoryId IS NOT NULL

""")

# Silver path
silver_path = "./datalake/silver/EndInv"

# if silver path doesnt exist, let's create it
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
