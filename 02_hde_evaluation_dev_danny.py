# Databricks notebook source
# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_1

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_2

# COMMAND ----------

from instore_eval import get_cust_activated, get_cust_movement, get_cust_brand_switching_and_penetration, get_cust_sku_switching, get_profile_truprice, get_customer_uplift, get_cust_cltv

# COMMAND ----------

import inspect
src_txt = inspect.getsource(get_customer_uplift)
print(src_txt)

# COMMAND ----------

cmp_id = "2022_0399_M01E"
cmp_start = "2022-06-01"
cmp_end = "2022-07-31"

# COMMAND ----------

txn_all = spark.table(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
cmp_st_date = datetime.strptime(cmp_start, '%Y-%m-%d')
cmp_end_date = datetime.strptime(cmp_end, '%Y-%m-%d')
sku_file = "upc_list_2022_0399_M01E.csv"
target_file = "target_store_2022_0399_M01E.csv"
cate_lvl = "subclass"
ai_file = "exposure_category_grouping_wth_subclass_code_20220101.csv"

# COMMAND ----------

feat_pd = pd.read_csv(os.path.join("/dbfs/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files", sku_file))
feat_list = feat_pd['feature'].drop_duplicates().to_list()

std_ai_df = spark.read.csv(os.path.join("dbfs:/FileStore/media/campaign_eval/00_std_inputs", ai_file), header="true", inferSchema="true")

cross_cate_flag = None
cross_cate_cd = None

feat_df, brand_df, class_df, sclass_df, cate_df, use_ai_df, \
brand_list, sec_cd_list, sec_nm_list, class_cd_list, class_nm_list, \
sclass_cd_list, sclass_nm_list, mfr_nm_list, cate_cd_list, \
use_ai_group_list, use_ai_sec_list = _get_prod_df(feat_list, cate_lvl, std_ai_df, cross_cate_flag, cross_cate_cd)

# COMMAND ----------


