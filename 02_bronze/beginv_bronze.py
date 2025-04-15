from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ingesta Bronze Purchases") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

INPUT_FOLDER = "./datalake/prelanding/BegInv"
OUTPUT_FOLDER = "./datalake/bronze/BegInv"
#basicamente tomamos la info de la prelanding cruda la convertimos en delta y guardamos en la zona bronze
try:
    df = spark.read.option("header", True).csv(INPUT_FOLDER)
    df.write.format("delta").mode("overwrite").save(OUTPUT_FOLDER)
except Exception as e:
    raise e