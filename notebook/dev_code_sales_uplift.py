# Databricks notebook source
import os
import sys

from pyspark.sql import functions as F

# COMMAND ----------

from utils.DBPath import DBPath, save_PandasDataFrame_to_csv_FileStore
from utils.campaign_config import CampaignConfigFile, CampaignEval
from utils.helper import to_pandas

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
load_txn.backward_convert_txn_schema(cmp)

# COMMAND ----------

cmp.txn.printSchema()

# COMMAND ----------

from matching import store_matching

# COMMAND ----------

# save_PandasDataFrame_to_csv_FileStore(cmp.matched_store.toPandas(), cmp.output_path/"output"/"store_matching.csv")

# COMMAND ----------

store_matching.get_store_matching_across_region(cmp)
store_matching.backward_convert_matching_schema(cmp)

# COMMAND ----------

cmp.matched_store.display()

# COMMAND ----------

# DBTITLE 1,Dev
# MAGIC %run /Repos/thanakrit.boonquarmdee@lotuss.com/edm_media_dev/notebook/dev_notebook_fn

# COMMAND ----------

feat_list = cmp.feat_sku.toPandas()["upc_id"].to_numpy().tolist()
# cmp.txn = cmp.txn.withColumn("pkg_weight_unit", F.col("unit"))
# cmp.txn = cmp.txn.replace({"dur":"cmp"}, subset=['period_fis_wk', 'period_promo_wk', 'period_promo_mv_wk'])

matching_df = to_pandas(cmp.matched_store)
from pyspark.sql.functions import broadcast

info, tab, _, _, _, _ = sales_uplift_reg_mech(cmp.txn, "sku", cmp.feat_brand_sku, feat_list, matching_df)

# COMMAND ----------

info.display()

# COMMAND ----------

tab.display()

# COMMAND ----------


