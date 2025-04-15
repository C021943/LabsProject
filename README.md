# Proyecto Base Labs - Arquitectura de Datos

## Descripción General
Este proyecto tiene como objetivo construir una arquitectura de datos local, automatizada y escalable, que simule un Data Lake y que permita extraer, transformar y cargar información desde archivos publicados por PwC. El resultado final es la generación de tablas de negocio que se consumen en tableros visuales en Sigma.

## Requisitos
- Python 3 y Visual Studio Code
- Docker instalado y funcionando
- Dependencias listadas en `requirements.txt`
- Contenedor de Spark configurado en `docker-compose.yml`

## Stack Tecnológico
- **Python** para desarrollo general y automatización
- **Docker** para correr PySpark + Delta Lake sin dependencias locales
- **Apache Spark + Delta Lake** para procesamiento de datos
- **PostgreSQL (Railway)** como base intermedia para consumo
- **Sigma** como herramienta de visualización

## Arquitectura
El proyecto simula un Data Lake local, dividido en capas:

### 1. Prelanding
- Script: `01_prelanding/Template_Ingestion.py`
- Descarga y organiza los archivos `.zip` desde el sitio oficial de PwC
- Extrae archivos CSV y los guarda en sus carpetas correspondientes

### 2. Bronze
- Almacena los datos crudos en formato Delta Lake
- Scripts en `02_bronze/`
- No aplica transformaciones, solo conversión a Delta

### 3. Silver
- Limpieza y normalización de datos
- Se descartan tablas innecesarias, como asi también nulos y columnas innecesarias
- Scripts en `03_silver/`

### 4. Gold
- Generación de tablas de negocio: `margins_profits`, `margins_profits_brand`, `rotation`
- Scripts en `04_gold/`

### 5. PostgreSQL
- Base Postgresql alojada en Railway para exponer datos a Sigma
- Scripts en `05_postgres/`

### 6. Sigma
- Consumo de datos desde PostgreSQL para visualización de insights

### Pipeline
- Script: `pipelines/ppl.py`
- Automatiza la ejecución de todo el flujo desde Prelanding hasta PostgreSQL
