# Databricks notebook source
import os
import sys
import pandas as pd

from pyspark.sql import functions as F

# COMMAND ----------

from utils.campaign_config import CampaignConfigFile, CampaignEvalO3, CampaignEval
from utils.helper import timer

# COMMAND ----------

inst = CampaignConfigFile("/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/01_hde/00_cmp_inputs/cmp_list_pakc_new.csv")

# o3 = CampaignConfigFile("/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/05_O3/00_cmp_inputs/cmp_list_o3_than.csv")

# COMMAND ----------

inst.display_details()

# COMMAND ----------

inst_eval = CampaignEval(inst, cmp_row_no=97)

# COMMAND ----------

from utils import load_txn
load_txn.load_txn(inst_eval, txn_mode="pre_generated_118wk")

# COMMAND ----------

inst_eval.display_details()

# COMMAND ----------

from 

# COMMAND ----------

import polars as pl
pf = pl.read_json('/dbfs/mnt/pvtdmbobazc01/edminput/filestore/share/media/campaign_eval/01_hde/Sep_2023/2023_0770_M02C_mirinda_shelftalker/output/params.json')
display(pf)

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
