# Databricks notebook source
import os
import sys
import pandas as pd

from pyspark.sql import functions as F

# COMMAND ----------

from utils.campaign_config import CampaignConfigFile, CampaignEvalO3, CampaignEval
from utils.helper import timer

# COMMAND ----------

inst = CampaignConfigFile("/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_hde_than.csv")

# o3 = CampaignConfigFile("/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/05_O3/00_cmp_inputs/cmp_list_o3_than.csv")

# COMMAND ----------

inst.display_details()

# COMMAND ----------

inst_eval = CampaignEval(inst, cmp_row_no=12)

# COMMAND ----------

o3.display_details()

# COMMAND ----------

o3_eval = CampaignEvalO3(o3, cmp_row_no=1)

# COMMAND ----------

isinstance(o3_eval, CampaignEvalO3)

# COMMAND ----------

o3_eval.params

# COMMAND ----------

o3_eval.target_store.display()

# COMMAND ----------

from utils import load_txn

# COMMAND ----------

load_txn.load_txn(o3_eval, txn_mode="pre_generated_118wk")

# COMMAND ----------

from exposure import exposed

# COMMAND ----------

exposure_all, exposure_reg, exposure_mech = exposed.get_exposure(o3_eval)

# COMMAND ----------

exposure_all.display()

# COMMAND ----------

exposure_reg.display()

# COMMAND ----------

exposure_mech.display()

# COMMAND ----------

o3_eval.params
