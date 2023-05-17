# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile(
    "/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than_04_2023.csv"
)

# COMMAND ----------

conf.display_details()

# COMMAND ----------

conf.search_details(column="cmp_nm", search_key="0019")

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=2)

# COMMAND ----------

cmp.params

# COMMAND ----------

from utils import exposure
from utils import load_txn
from utils import activated

load_txn.load_txn(cmp, txn_mode="pre_generated_118wk")

# COMMAND ----------

brand_activated, sku_activated, brand_activated_sales_df, sku_activated_sales_df = activated.get_cust_activated(cmp)

# COMMAND ----------

brand_activated_by_mech, sku_activated_by_mech, agg_numbers_by_mech = activated.get_cust_activated_by_mech(cmp)

# COMMAND ----------


