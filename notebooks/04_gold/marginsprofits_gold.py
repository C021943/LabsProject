from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Crear tabla Gold: MarginsProfits") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# tablas de la capa silver
df_sales = spark.read.format("delta").load("./datalake/silver/Sales")
df_purchase = spark.read.format("delta").load("./datalake/silver/PurchasesFINAL")
df_sales.createOrReplaceTempView("Sales")
df_purchase.createOrReplaceTempView("purchases")

# total compras por vendor y producto
spark.sql("""
    SELECT
      trim(VendorName) as vendorname,
      trim(description) as description,
      SUM(PurchasePrice * PurchaseQuantity) AS total_purchases
    FROM purchases
    GROUP BY trim(VendorName),
             trim(description)""").createOrReplaceTempView('Total_purchases')

# total ventas por vendor y producto, incluyendo el tax
spark.sql("""
    SELECT
      trim(VendorName) as vendorname,
      trim(description) as description,
      SUM(SalesPrice * SalesQuantity) AS total_sales,
      SUM(ExciseTax) AS total_tax
    FROM Sales
    GROUP BY trim(vendorname),
             trim(description)
""").createOrReplaceTempView('Total_sales')

# joins
df_final = spark.sql("""
    SELECT 
        s.vendorname,
        s.description,
        s.total_sales,
        s.total_tax,
        p.total_purchases,
        s.total_sales - s.total_tax - p.total_purchases AS profit,
        ((s.total_sales - s.total_tax) / p.total_purchases) - 1 AS margin
    FROM Total_purchases p
    INNER JOIN Total_sales s
        ON p.description = s.description
           AND p.vendorname = s.vendorname
        
""")
df_final.show()
#guardado
df_final.write.format("delta").mode("overwrite").save("./datalake/gold/MarginsProfits")
