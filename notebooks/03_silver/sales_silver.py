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
#UPSERT
if not os.path.exists(silver_path):
    df_cleaned.write.format("delta").mode("overwrite").save(silver_path)
else:
    silver_table = DeltaTable.forPath(spark, silver_path)
    silver_table.alias("target").merge(
        df_cleaned.alias("source"),
        """
        target.inventory_id = source.inventory_id
        AND target.sales_date = source.sales_date
        AND target.sales_dollars = source.sales_dollars
        AND target.description = source.description
        AND target.vendorname = source.vendorname
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()