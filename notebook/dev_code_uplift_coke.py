# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath, save_PandasDataFrame_to_csv_FileStore
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile("/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/02_gofresh/00_cmp_inputs/cmp_list_gf_than.csv")

# COMMAND ----------

conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=5)

# COMMAND ----------

# mockup Pandas dataframe
import pandas as pd

df = pd.DataFrame({"k":["a", "b"], "v":[1, 2]})
df

# COMMAND ----------

# Test save to output
save_PandasDataFrame_to_csv_FileStore(df, cmp.output_path/"output"/"test_save.csv")

# COMMAND ----------

# Test save to resutl
save_PandasDataFrame_to_csv_FileStore(df, cmp.output_path/"result"/"test_save.csv")

# COMMAND ----------

cmp.load_aisle(aisle_mode="target_store_config")

# COMMAND ----------

cmp.params

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(cmp, txn_mode="pre_generated_118wk")

# COMMAND ----------

cmp.params

# COMMAND ----------

cmp.txn.printSchema()

# COMMAND ----------

from uplift import uplift

# COMMAND ----------

cmp.txn = spark.table("tdm_seg.media_campaign_eval_txn_data_2023_0102_m_all").replace({"cmp":"dur"}).withColumn("unit", F.col("pkg_weight_unit"))

# COMMAND ----------

cmp.txn.printSchema()

# COMMAND ----------

cust_uplift = uplift.get_cust_uplift_by_mech(cmp, cmp.feat_brand_sku, "brand")

# COMMAND ----------

cust_uplift.display()

# COMMAND ----------

from exposure import exposed

# COMMAND ----------

cmp.txn = cmp.txn.withColumnRenamed("store_format_group", "store_format_name")

# COMMAND ----------

exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(cmp)

# COMMAND ----------

exposure_mech.display()

# COMMAND ----------

exposure_all.display()

# COMMAND ----------

exposure_reg.display()

# COMMAND ----------


