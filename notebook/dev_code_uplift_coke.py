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

cmp.target_store.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended tdm_dev.th_lotuss_media_eval_aisle_target_store_conf_2023_0102_m07c_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct upc_id)
# MAGIC from tdm_dev.th_lotuss_media_eval_aisle_target_store_conf_2023_0102_m07c_temp;

# COMMAND ----------


