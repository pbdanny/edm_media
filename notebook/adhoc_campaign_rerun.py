# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath
from utils.campaign_config import CampaignConfigFile, CampaignEval

# COMMAND ----------

list_of_fresh_zone = [
    "2022_0526_m01e_dettol_extra_display_at_fresh_midoct22",
    "2022_0526_m01e_v2_dettol_extra_space_at_fresh",
    "2022_0534_m01e_kireikirei_extradisplayatfresh",
    "2022_0565_m01e_listerine_extradisplayatfresh_vegetable01",
    "2022_0979_m01e_colgate_extradisplayatfresh_fruit02"
    ]

upper_list = [f.upper() for f in list_of_fresh_zone]

# COMMAND ----------

# Load all campaign config file into pandas

from pathlib import Path
import pandas as pd

conf_path = Path("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs")
conf_files = conf_path.glob("*.csv")
dfs = []
for f in conf_files:
    df = pd.read_csv(f)
    df["conf_files"] = f
    dfs.append(df)
conf_df = pd.concat(dfs)

# COMMAND ----------

pd.set_option('display.max_colwidth', 255)

conf_df[conf_df["cmp_id"].str.upper().isin(["2022_0526_M01E", "2022_0526_M01E_ADHOC", "2022_0534_M01E", "2022_0565_M01E", "2022_0979_M01E"])].loc[:, ["cmp_id", "cmp_nm", "cmp_start", "cmp_end", "conf_files"]].sort_values("cmp_id")

# COMMAND ----------

# conf_df[conf_df["cmp_id"].str.upper().isin(["2022_0526_M01E", "2022_0526_M01E_ADHOC", "2022_0534_M01E", "2022_0565_M01E", "2022_0979_M01E"])].to_csv("/dbfs/FileStore/thanakrit/temp/to_export/disp_at_fresh.csv", index=False)

# COMMAND ----------

conf = CampaignConfigFile(
    "/dbfs/FileStore/media/campaign_eval/03_promozone/00_cmp_inputs/cmp_list_pz_extrafresh.csv"
)

# COMMAND ----------

conf.display_details()

# COMMAND ----------

from utils import cleanup
cmp = CampaignEval(conf, cmp_row_no=5)
cleanup.clear_attr_and_temp_tbl(cmp)

# COMMAND ----------

cmp.target_store.display()
cmp.load_aisle(aisle_mode="target_store_config")

# COMMAND ----------

cmp.params

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


