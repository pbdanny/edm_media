# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath, save_PandasDataFrame_to_csv_FileStore
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_niti.csv")
# conf = CampaignConfigFile("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_pakc_multi.csv")

# COMMAND ----------

conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=24)

# COMMAND ----------

cmp.load_aisle()

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(cmp, txn_mode="stored_campaign_txn")

# COMMAND ----------

from matching import store_matching

# COMMAND ----------

# save_PandasDataFrame_to_csv_FileStore(cmp.matched_store.toPandas(), cmp.output_path/"output"/"store_matching.csv")

# COMMAND ----------

store_matching.get_store_matching_across_region(cmp)

# COMMAND ----------

cmp.matched_store.display()

# COMMAND ----------

# DBTITLE 1,Dev
# MAGIC %run /Repos/thanakrit.boonquarmdee@lotuss.com/edm_media_dev/notebook/dev_notebook_fn

# COMMAND ----------

feat_list = cmp.feat_sku.toPandas()["upc_id"].to_numpy().tolist()
cmp.txn = cmp.txn.withColumn("pkg_weight_unit", F.col("unit"))
matching_df = cmp.matched_store.toPandas()
sales_uplift_reg_mech(cmp.txn, "sku", cmp.feat_brand_sku, feat_list, cmp.matched_store.toPandas())

# COMMAND ----------


