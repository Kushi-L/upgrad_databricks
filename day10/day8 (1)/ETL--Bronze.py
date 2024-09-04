# Databricks notebook source
# MAGIC %run
# MAGIC "/Workspace/Users/kushi_1724384751548@npupgradassessment.onmicrosoft.com/day10/day8 (1)/includes"

# COMMAND ----------

input_path = "/FileStore/products.csv"
df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)
df1 = add_ingestion_col(df)
df1.write.mode("overwrite").saveAsTable("bronze.products_bronze")

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("environment","dev")
v=dbutils.widgets.get("environment")

# COMMAND ----------

df=spark.read.csv(f"dbfs:/FileStore/products.csv",header=True,inferSchema=True)
df1=add_ingestion_col(df)
df2=df1.withColumn("environment",lit(v))
df2.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("bronze.products_bronze")
