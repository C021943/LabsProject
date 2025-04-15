import subprocess

# ejecutamos el script de prelanding (corre fuera del contenedor)
print("Ejecutando Template_Ingestion.py local...")
subprocess.run(["python", "notebooks/01_prelanding/Template_Ingestion.py"], check=True)

# comando base para que corra dentro del contenedor
base_command = [
    "docker", "exec", "spark-delta",
    "spark-submit",
    "--packages", "io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.5.0",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
]

# BRONZE
scripts_bronze = [
    "notebooks/02_bronze/beginv_bronze.py",
    "notebooks/02_bronze/endinv_bronze.py",
    "notebooks/02_bronze/invoicepurchases_bronze.py",
    "notebooks/02_bronze/purchasesfinal_bronze.py",
    "notebooks/02_bronze/purchasesprices_bronze.py",
    "notebooks/02_bronze/sales_bronze.py",
]

# SILVER
scripts_silver = [
    "notebooks/03_silver/beginv_silver.py",
    "notebooks/03_silver/endinv_silver.py",
    "notebooks/03_silver/purchasesfinal_silver.py",
    "notebooks/03_silver/sales_silver.py",
]

# GOLD
scripts_gold = [
    "notebooks/04_gold/marginsprofits_gold.py",
    "notebooks/04_gold/rotation_gold.py"
]

# POSTGRES
script_postgres = "notebooks/05_postgres/marginsprofits_postgres.py"

# Ejecución BRONZE
print("✅✅✅✅✅✅ PROCESO BRONZE ✅✅✅✅✅✅")
for script in scripts_bronze:
    print(f">>> {script}")
    subprocess.run(base_command + [script], check=True)

# Ejecución SILVER
print("✅✅✅✅✅✅ PROCESO SILVER ✅✅✅✅✅✅")
for script in scripts_silver:
    print(f">>> {script}")
    subprocess.run(base_command + [script], check=True)

# Ejecución GOLD
print("✅✅✅✅✅✅ PROCESO GOLD ✅✅✅✅✅✅")
for script in scripts_gold:
    print(f">>> {script}")
    subprocess.run(base_command + [script], check=True)

# Ejecución POSTGRES
print("✅✅✅✅✅✅ PROCESO POSTGRES ✅✅✅✅✅✅")
print(f">>> {script_postgres}")
subprocess.run(base_command + [script_postgres], check=True)

print("✅✅✅✅✅✅ PROCESO COMPLETO ✅✅✅✅✅✅")
