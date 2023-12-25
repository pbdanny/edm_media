# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath, save_PandasDataFrame_to_csv_FileStore
from utils.campaign_config import CampaignConfigFile, CampaignEval
from utils.helper import to_pandas

# COMMAND ----------

# conf = CampaignConfigFile("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_08.csv")
conf = CampaignConfigFile("/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than.csv")

# COMMAND ----------

conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=1)

# COMMAND ----------

cmp.feat_brand_nm.display()

# COMMAND ----------

cmp.target_store.display()

# COMMAND ----------

cmp.aisle_target_store_conf.display()

# COMMAND ----------

from exposure import exposed

exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(cmp)

# COMMAND ----------

cmp.params

# COMMAND ----------

from exposure import exposed

# COMMAND ----------

exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(cmp)

# COMMAND ----------

exposure_all.display()

# COMMAND ----------

cmp.params

# COMMAND ----------

from uplift import uplift

# COMMAND ----------

x = uplift.get_cust_uplift_by_mech(cmp, cmp.feat_sku, "sku")

# COMMAND ----------

x.display()

# COMMAND ----------

spark.sql("show tables in tdm_seg like 'th_lotuss_media*'").display()

# COMMAND ----------

from utils import cleanup

# COMMAND ----------

cleanup.clear_attr_and_temp_tbl(cmp)
