# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_2023_06_dev.csv")

# COMMAND ----------

conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=1)

# COMMAND ----------

cmp.target_store.display()
cmp.load_aisle(aisle_mode="target_store_config")

# COMMAND ----------

cmp.params

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(cmp, txn_mode="pre_generated_118wk")

# COMMAND ----------

from exposure import exposed

# COMMAND ----------

exp_all, exp_reg, exp_mehc = exposed.get_exposure(cmp)

# COMMAND ----------

exp_all.display()

# COMMAND ----------

from activate import activated
act = activated.get_cust_by_mech_exposed_purchased(cmp, cmp.feat_sku, "sku")

# COMMAND ----------

act.display()

# COMMAND ----------

act.agg(F.count("*"), F.count_distinct("household_id")).display()

# COMMAND ----------

from uplift import uplift
uplift = uplift.get_cust_uplift_by_mech(cmp, cmp.feat_sku, "sku")

# COMMAND ----------

uplift.display()

# COMMAND ----------


