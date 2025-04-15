from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Crear tabla Gold: MarginsProfits") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# tablas de la capa silver
df_sales = spark.read.format("delta").load("./datalake/silver/BegInv")
df_purchase = spark.read.format("delta").load("./datalake/silver/EndInv")
df_sales.createOrReplaceTempView("BegInv")
df_purchase.createOrReplaceTempView("EndInv")


df_beg = spark.sql("""
SELECT description, sum(onHand) as onHand
                   from BegInv
      group by description
                   
        
""").createOrReplaceTempView("BegInvtotal")

df_end = spark.sql("""
SELECT description, sum(onHand) as onHand
                   from EndInv
      group by description 
""").createOrReplaceTempView("EndInvtotal")

# joins
df_final = spark.sql("""

SELECT b.description,
       b.onHand as initial_inv,
       e.onHand as final_inv
from  BegInvtotal b inner join EndInvtotal e on (b.description = e.description)
                     
""")

df_final.show()

#guardado
df_final.write.format("delta").mode("overwrite").save("./datalake/gold/Rotation")
