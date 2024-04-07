# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

conf = CampaignConfigFile(
    "/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than.csv"
)

# COMMAND ----------

conf.display_details()

# COMMAND ----------

cmp = CampaignEval(conf, cmp_row_no=5)

# COMMAND ----------

cmp.save_details()

# COMMAND ----------

cmp.target_store.display()
cmp.load_aisle(aisle_mode="target_store_config")

# COMMAND ----------

cmp.product_dim.where(F.col("subclass_code").isin(["1_2_157_1_19"])).display()

# COMMAND ----------

cmp.aisle_target_store_conf.display()

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(cmp, txn_mode="pre_generated_118wk")

# COMMAND ----------

# DBTITLE 1,Cross Cate 
from cross_cate import asso_basket

# COMMAND ----------

a, b, c = asso_basket.get_asso_kpi(cmp, cmp.feat_sku)

# COMMAND ----------

d, e, f = asso_basket.get_asso_kpi(cmp, cmp.feat_brand_sku)

# COMMAND ----------

a.display()

# COMMAND ----------

b.display()

# COMMAND ----------

c.display()

# COMMAND ----------

d.display()

# COMMAND ----------

e.display()

# COMMAND ----------

f.display()

# COMMAND ----------


