# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

upc = spark.read.csv("dbfs:/FileStore/media/campaign_eval/04_O2O/00_cmp_inputs/input_files/upc_list_2023_0089_M01C.csv", header=True, inferSchema=True).withColumnRenamed("feature", "upc_id")

prd_c = spark.table(TBL_PROD)

prd_c.join(upc, "upc_id", "inner").display()

# COMMAND ----------

brand = spark.read.csv("dbfs:/FileStore/media/campaign_eval/04_O2O/00_cmp_inputs/input_files/upc_list_brand_2022_0744_M01C.csv", header=True, inferSchema=True)

# COMMAND ----------


