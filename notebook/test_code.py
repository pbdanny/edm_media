# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile(
    "/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_pakc_new.csv"
)

# COMMAND ----------

conf.display_details()

# COMMAND ----------

conf.search_details(column="cmp_id", search_key="2023_0102")

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=70)

# COMMAND ----------

cmp.target_store.display()
cmp.load_aisle(aisle_mode="target_store_config")

# COMMAND ----------

cmp.params

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(cmp, txn_mode="pre_generated_118wk")

# COMMAND ----------

# DBTITLE 1,Exposure
from exposure import exposed

# COMMAND ----------

cmp_exposure_all, cmp_exposure_region, cmp_exposure_mech = exposed.get_exposure(cmp)

# COMMAND ----------

cmp_exposure_all.display()

# COMMAND ----------

cmp_exposure_mech.display()

# COMMAND ----------

# DBTITLE 1,Activated : Exposed & Purchased
from activate.activated import get_cust_by_mach_activated

brand_activated, sku_activated, summary = get_cust_by_mach_activated(cmp)

# COMMAND ----------

brand_activated.display(10)

# COMMAND ----------

# DBTITLE 1,Dev : Uplift


# COMMAND ----------

from uplift import uplift

# COMMAND ----------

sku_uplift = uplift.get_cust_uplift_any_mech(cmp, cmp.feat_sku, "sku")
