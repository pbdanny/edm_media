# Databricks notebook source
# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_1

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_2

# COMMAND ----------

from instore_eval import get_cust_activated, get_cust_movement, get_cust_brand_switching_and_penetration, get_cust_sku_switching, get_profile_truprice, get_customer_uplift, get_cust_cltv, get_cust_activated_by_mech, get_customer_uplift_by_mech, get_customer_uplift_per_mechanic

# COMMAND ----------

cmp_id = "2022_0012_M01M"
cmp_start = "2022-06-01"
cmp_end = "2022-06-30"
gap_start_date = ""
gap_end_date = ""
cmp_nm = "2022_0012_M01M_Nescafe_Shelf_Divider"

txn_all = spark.table(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')
cmp_st_date = datetime.strptime(cmp_start, '%Y-%m-%d')
cmp_end_date = datetime.strptime(cmp_end, '%Y-%m-%d')
sku_file = "upc_list_2022_0012_M01M.csv"
cate_lvl = "subclass"
ai_file = "exposure_category_grouping_wth_subclass_code_20220101.csv"

# COMMAND ----------


cmp_st_wk   = wk_of_year_ls(cmp_start)
cmp_en_wk   = wk_of_year_ls(cmp_end)
 
## promo_wk
cmp_st_promo_wk   = wk_of_year_promo_ls(cmp_start)
cmp_en_promo_wk   = wk_of_year_promo_ls(cmp_end)
 
## Gap Week (fis_wk)
if ((str(gap_start_date).lower() == 'nan') | (str(gap_start_date).strip() == '')) & ((str(gap_end_date).lower == 'nan') | (str(gap_end_date).strip() == '')):
    print('No Gap Week for campaign :' + str(cmp_nm))
    gap_flag    = False
    chk_pre_wk  = cmp_st_wk
    chk_pre_dt  = cmp_start
elif( (not ((str(gap_start_date).lower() == 'nan') | (str(gap_start_date).strip() == ''))) & 
      (not ((str(gap_end_date).lower() == 'nan')   | (str(gap_end_date).strip() == ''))) ):    
    print('\n Campaign ' + str(cmp_nm) + ' has gap period between : ' + str(gap_start_date) + ' and ' + str(gap_end_date) + '\n')
    ## fis_week
    gap_st_wk   = wk_of_year_ls(gap_start_date)
    gap_en_wk   = wk_of_year_ls(gap_end_date)
    
    ## promo
    gap_st_promo_wk  = wk_of_year_promo_ls(gap_start_date)
    gap_en_promo_wk  = wk_of_year_promo_ls(gap_end_date)
    
    gap_flag         = True    
    
    chk_pre_dt       = gap_start_date
    chk_pre_wk       = gap_st_wk
    chk_pre_promo_wk = gap_st_promo_wk
    
else:
    print(' Incorrect gap period. Please recheck - Code will skip !! \n')
    print(' Received Gap = ' + str(gap_start_date) + " and " + str(gap_end_date))
    raise Exception("Incorrect Gap period value please recheck !!")
## end if   
 
pre_en_date = (datetime.strptime(chk_pre_dt, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
pre_en_wk   = wk_of_year_ls(pre_en_date)
pre_st_wk   = week_cal(pre_en_wk, -12)                       ## get 12 week away from end week -> inclusive pre_en_wk = 13 weeks
pre_st_date = f_date_of_wk(pre_st_wk).strftime('%Y-%m-%d')   ## get first date of start week to get full week data
## promo week
pre_en_promo_wk = wk_of_year_promo_ls(pre_en_date)
pre_st_promo_wk = promo_week_cal(pre_en_promo_wk, -12)   
 
ppp_en_wk       = week_cal(pre_st_wk, -1)
ppp_st_wk       = week_cal(ppp_en_wk, -12)
##promo week
ppp_en_promo_wk = promo_week_cal(pre_st_promo_wk, -1)
ppp_st_promo_wk = promo_week_cal(ppp_en_promo_wk, -12)
 
ppp_st_date = f_date_of_wk(ppp_en_wk).strftime('%Y-%m-%d')
ppp_en_date = f_date_of_wk(ppp_st_wk).strftime('%Y-%m-%d')

# COMMAND ----------

target_file = "target_store_2022_0012_M01M_sep_mech.csv"
test_store_sf = spark.read.csv(os.path.join("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files", target_file), header=True, inferSchema=True)
test_store_sf.display()
test_store_sf.groupBy("mech_name").count().display()

# COMMAND ----------

txn_all = spark.table(f'tdm_seg.media_campaign_eval_txn_data_{cmp_id}')

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

type(cate_cd_list)

# COMMAND ----------

store_matching_df = pd.read_csv("/dbfs/FileStore/media/campaign_eval/01_hde/Jun_2022/2022_0012_M01M_Nescafe_Shelf_Divider/output/store_matching.csv")
ctr_store_list = list(set([s for s in store_matching_df.ctr_store_var]))

# COMMAND ----------

cp_start_date=cmp_st_date
cp_end_date=cmp_end_date
txn = txn_all
adj_prod_sf = use_ai_df

# COMMAND ----------

# MAGIC %md ##Test Ta's Code

# COMMAND ----------

uplift_feature, exposed_unexposed_buy_flag_by_mech_sku = get_customer_uplift_per_mechanic(txn=txn_all, 
                                                                                           cp_start_date=cmp_st_date, 
                                                                                           cp_end_date=cmp_end_date,
                                                                                           wk_type="fis_week",
                                                                                           test_store_sf=test_store_sf,
                                                                                           adj_prod_sf=use_ai_df, 
                                                                                           brand_sf=brand_df,
                                                                                           feat_sf=feat_df,
                                                                                           ctr_store_list=ctr_store_list,
                                                                                           cust_uplift_lv="brand")

# COMMAND ----------

# MAGIC %md ## Test activated by mech

# COMMAND ----------

# brand_activated, sku_activated = get_cust_activated_by_mech(txn_all,
#                                                     cmp_start,
#                                                     cmp_end,
#                                                     "fis_week",
#                                                     test_store_sf,
#                                                     adj_prod_sf,
#                                                     brand_df,
#                                                     feat_df)

# COMMAND ----------

# display(brand_activated.agg(F.count_distinct("household_id")))

# COMMAND ----------

# uplift_brand = get_customer_uplift_by_mech(txn=txn_all, 
#                                    cp_start_date=cmp_st_date, 
#                                    cp_end_date=cmp_end_date,
#                                    wk_type="fis_week",
#                                    test_store_sf=test_store_sf,
#                                    adj_prod_sf=adj_prod_sf, 
#                                    brand_sf=brand_df,
#                                    feat_sf=feat_df,
#                                    ctr_store_list=ctr_store_list,
#                                    cust_uplift_lv="brand")

# COMMAND ----------

uplift_brand.display()
