# Databricks notebook source
# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_1

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/_campaign_eval_utils_2

# COMMAND ----------

from instore_eval import get_cust_activated, get_cust_movement, get_cust_brand_switching_and_penetration, get_cust_sku_switching, get_profile_truprice, get_customer_uplift, get_cust_cltv

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

uplift_brand = get_customer_uplift(txn=txn_all, 
                                   cp_start_date=cmp_st_date, 
                                   cp_end_date=cmp_end_date,
                                   wk_type="fis_week",
                                   test_store_sf=test_store_sf,
                                   adj_prod_sf=adj_prod_sf, 
                                   brand_sf=brand_df,
                                   feat_sf=feat_df,
                                   ctr_store_list=ctr_store_list,
                                   cust_uplift_lv="brand")

uplift_brand_df = to_pandas(uplift_brand)

# COMMAND ----------

store_matching_df = pd.read_csv("/dbfs/FileStore/media/campaign_eval/01_hde/Jun_2022/2022_0012_M01M_Nescafe_Shelf_Divider/output/store_matching.csv")
ctr_store_list = list(set([s for s in store_matching_df.ctr_store_var]))

# COMMAND ----------

def _create_ctrl_store_sf(ctr_store_list: List,
                         cp_start_date: str,
                         cp_end_date: str
                         ) -> SparkDataFrame:
    """From list of control store, fill c_start, c_end
    based on cp_start_date, cp_end_date
    """
    df = pd.DataFrame(ctr_store_list, columns=["store_id"])
    sf = spark.createDataFrame(df)  # type: ignore

    filled_ctrl_store_sf = \
        (sf
         .withColumn("c_start", F.lit(cp_start_date))
         .withColumn("c_end", F.lit(cp_end_date))
         .withColumn("mech_name", F.lit("ctrl_store"))
        )
    return filled_ctrl_store_sf

def _create_test_store_sf(test_store_sf: SparkDataFrame,
                         cp_start_date: str,
                         cp_end_date: str
                         ) -> SparkDataFrame:
    """From target store definition, fill c_start, c_end
    based on cp_start_date, cp_end_date
    """
    filled_test_store_sf = \
        (test_store_sf
        .fillna(str(cp_start_date), subset='c_start')
        .fillna(str(cp_end_date), subset='c_end')
        )
    return filled_test_store_sf
    
def _get_exposed_cust(txn: SparkDataFrame,
                      test_store_sf: SparkDataFrame,
                      adj_prod_sf: SparkDataFrame,
                      channel: str = "OFFLINE"
                      ) -> SparkDataFrame:
    """Get exposed customer & first exposed date
    """
    out = \
        (txn
         .where(F.col("channel")==channel)
         .where(F.col("household_id").isNotNull())
         .join(test_store_sf, "store_id", "inner") # Mapping cmp_start, cmp_end, mech_count, mech_name by store
         .join(adj_prod_sf, "upc_id", "inner")
         .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
         .select("household_id", "mech_name", F.col("transaction_uid").alias("exposed_txn_id"), F.col("tran_datetime").alias("exposed_datetime"))
         .drop_duplicates()
        )
    return out

def _get_shppr(txn: SparkDataFrame,
               period_wk_col_nm: str,
               prd_scope_df: SparkDataFrame
               ) -> SparkDataFrame:
    """Get first brand shopped date or feature shopped date, based on input upc_id
    Shopper in campaign period at any store format & any channel
    """
    out = \
        (txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col_nm).isin(["cmp"]))
         .join(prd_scope_df, 'upc_id')
         .select('household_id', F.col("transaction_uid").alias("shp_txn_id"), F.col("tran_datetime").alias("shp_datetime"))
         .drop_duplicates()
        )
    return out

# COMMAND ----------

cp_start_date=cmp_st_date
cp_end_date=cmp_end_date
txn = txn_all
adj_prod_sf = use_ai_df

target_str = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
cmp_exposed = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)
cmp_shppr = _get_shppr(txn=txn, period_wk_col_nm="period_fis_wk", prd_scope_df=brand_df)

# COMMAND ----------

cmp_exposed_buy = \
(cmp_exposed
 .join(cmp_shppr, "household_id", "left")
 .withColumn("exp_x_shp", F.count("*").over(Window.partitionBy("household_id")))
 .withColumn("sec_diff", F.col("shp_datetime").cast("long") - F.col("exposed_datetime").cast("long"))
 .withColumn("n_mech_exp", F.size(F.collect_set("mech_name").over(Window.partitionBy("household_id"))))
 .withColumn("n_exp", F.size(F.collect_set("exposed_txn_id").over(Window.partitionBy("household_id"))))
 .withColumn("n_shp", F.size(F.collect_set("shp_txn_id").over(Window.partitionBy("household_id"))))
)

(cmp_exposed_buy
 .write
 .format("parquet")
 .mode("overwrite")
 .save("dbfs:/FileStore/thanakrit/temp/checkpoint/dev_cmp_exposed_buy.parquet")
)

# COMMAND ----------

cmp_exposed_buy.agg(F.count_distinct("household_id")).show()

# COMMAND ----------

cmp_exposed_buy.groupBy("mech_name").agg(F.count_distinct("household_id")).show()

# COMMAND ----------

flag_exposed_by_mech = \
(cmp_exposed_buy
 .where(F.col("sec_diff").isNotNull())
 .where(F.col("sec_diff")>=0)
 .withColumn("proximity_rank", 
             F.row_number().over(Window.partitionBy("household_id", "shp_txn_id")
                           .orderBy(F.col("sec_diff").asc_nulls_last())))
 .where(F.col("proximity_rank")==1)
 .select("household_id", "mech_name")
 .drop_duplicates()
)

# COMMAND ----------

flag_exposed_by_mech.groupBy("mech_name").agg(F.count_distinct("household_id")).show()

# COMMAND ----------

ctr_str = _create_ctrl_store_sf(ctr_store_list=ctr_store_list, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
cmp_unexposed = \
(_get_exposed_cust(txn=txn_all, test_store_sf=ctr_str, adj_prod_sf=adj_prod_sf)
 .withColumnRenamed("exposed_datetime", "unexposed_datetime")
 .withColumnRenamed("exposed_txn_id", "unexposed_txn_id")
)

cmp_shppr = _get_shppr(txn=txn, period_wk_col_nm="period_fis_wk", prd_scope_df=brand_df)

cmp_unexposed_buy = \
(cmp_unexposed
 .join(cmp_shppr, "household_id", "left")
 .withColumn("exp_x_shp", F.count("*").over(Window.partitionBy("household_id")))
 .withColumn("sec_diff", F.col("shp_datetime").cast("long") - F.col("unexposed_datetime").cast("long"))
 .withColumn("n_mech_unexp", F.size(F.collect_set("mech_name").over(Window.partitionBy("household_id"))))
 .withColumn("n_unexp", F.size(F.collect_set("unexposed_txn_id").over(Window.partitionBy("household_id"))))
 .withColumn("n_shp", F.size(F.collect_set("shp_txn_id").over(Window.partitionBy("household_id"))))
)

(cmp_unexposed_buy
 .write
 .format("parquet")
 .mode("overwrite")
 .save("dbfs:/FileStore/thanakrit/temp/checkpoint/dev_cmp_unexposed_buy.parquet")
)

# COMMAND ----------

cmp_unexposed_buy = spark.read.parquet("dbfs:/FileStore/thanakrit/temp/checkpoint/dev_cmp_unexposed_buy.parquet")
cmp_unexposed_buy.display()

# COMMAND ----------

cmp_unexposed_buy.groupby("mech_name").agg(F.count_distinct("household_id")).display()

# COMMAND ----------

flag_unexposed_by_mech = \
(cmp_unexposed_buy
 .where(F.col("sec_diff").isNotNull())
 .where(F.col("sec_diff")>=0)
 .withColumn("proximity_rank", 
             F.row_number().over(Window.partitionBy("household_id", "shp_txn_id")
                           .orderBy(F.col("sec_diff").asc_nulls_last())))
 .where(F.col("proximity_rank")==1)
 .select("household_id", "mech_name")
 .drop_duplicates()
)

flag_unexposed_by_mech.display()

# COMMAND ----------

flag_unexposed_by_mech.display()

# COMMAND ----------

flag_unexposed_by_mech.count()

# COMMAND ----------

# MAGIC %md ##Exposed - Shop

# COMMAND ----------

cmp_shppr.where(F.col("household_id")==102111060002423872).where(F.col("shp_txn_id")==123661720002).display()

# COMMAND ----------

cmp_exposed.where(F.col("household_id")==102111060002423872).where(F.col("exposed_txn_id")==123141440032).display()

# COMMAND ----------

(cmp_exposed
 .join(cmp_shppr, "household_id", "left")
 .where(F.col("household_id")==102111060002423872)
 .where(F.col("shp_txn_id")==123661720002)
 .where(F.col("exposed_txn_id")==123141440032)
).display()

# COMMAND ----------

cmp_exposed_buy = \
(cmp_exposed
 .join(cmp_shppr, "household_id", "left")
 .withColumn("exp_x_shp", F.count("*").over(Window.partitionBy("household_id")))
 .withColumn("sec_diff", F.col("shp_datetime").cast("long") - F.col("exposed_datetime").cast("long"))
 .withColumn("n_mech_exp", F.size(F.collect_set("mech_name").over(Window.partitionBy("household_id"))))
 .withColumn("n_exp", F.size(F.collect_set("exposed_txn_id").over(Window.partitionBy("household_id"))))
 .withColumn("n_shp", F.size(F.collect_set("shp_txn_id").over(Window.partitionBy("household_id"))))
)

(cmp_exposed_buy
 .write
 .format("parquet")
 .mode("overwrite")
 .save("dbfs:/FileStore/thanakrit/temp/checkpoint/dev_cmp_exposed_buy.parquet")
)

# COMMAND ----------

cmp_exposed_buy = spark.read.parquet("dbfs:/FileStore/thanakrit/temp/checkpoint/dev_cmp_exposed_buy.parquet")

(cmp_exposed_buy
 .where(F.col("household_id")==102111060002423872)
 .where(F.col("shp_txn_id")==123661720002)
 .where(F.col("exposed_txn_id")==123141440032)
).display()

# COMMAND ----------

cmp_exposed_buy = spark.read.parquet("dbfs:/FileStore/thanakrit/temp/checkpoint/dev_cmp_exposed_buy.parquet")
cmp_exposed_buy.where(F.col("n_mech_exp")>=2).where(F.col("n_exp")>5).where(F.col("n_shp")>5).display()

# COMMAND ----------

(cmp_exposed_buy
 .where(F.col("household_id")==102111060012190025)
 .where(F.col("sec_diff")>=0)
 .withColumn("proximity_rank", 
             F.row_number().over(Window.partitionBy("household_id", "shp_txn_id")
                           .orderBy(F.col("sec_diff").asc_nulls_last())))
 .where(F.col("proximity_rank")==1)
).display()

# COMMAND ----------

(cmp_exposed_buy
 .where(F.col("household_id")==102111060002423872)
 .where(F.col("sec_diff")>=0)
 .withColumn("proximity_rank", 
             F.row_number().over(Window.partitionBy("household_id", "shp_txn_id")
                           .orderBy(F.col("sec_diff").asc_nulls_last())))
 .where(F.col("proximity_rank")==1)
).display()

# COMMAND ----------

(cmp_exposed_buy
 .where(F.col("household_id")==102111060002423872)
 .where(F.col("sec_diff")<0)
#  .withColumn("proximity_rank", 
#              F.row_number().over(Window.partitionBy("household_id", "shp_txn_id")
#                            .orderBy(F.col("sec_diff").asc_nulls_last())))
#  .where(F.col("proximity_rank")==1)
).display()

# COMMAND ----------

flag_exposed_by_mech = \
(cmp_exposed_buy
 .where(F.col("sec_diff")>=0)
 .withColumn("proximity_rank", 
             F.row_number().over(Window.partitionBy("household_id", "shp_txn_id")
                           .orderBy(F.col("sec_diff").asc_nulls_last())))
 .where(F.col("proximity_rank")==1)
 .select("household_id", "mech_name")
 .drop_duplicates()
)

# COMMAND ----------

flag_exposed_by_mech.groupBy("mech_name").count().display()

# COMMAND ----------

unex

# COMMAND ----------

"""
(A)
Shop datetime - Exposed datetime = diff_time

if 
shop after exposed = positive
shop before exposed = negative , remove

(B)
Sort by diff time (ascending , null last)

(C)
Pick first row

"""

# COMMAND ----------

cmp_exposed_buy.where(F.col("exp_x_shp")==23).where(F.col("household_id")==102111060001864548).orderBy(F.col("sec_diff").asc_nulls_last()).display()

# COMMAND ----------

cmp_exposed_buy.groupBy("household_id").agg(F.count("*").alias("n")).groupBy("n").count().display()

# COMMAND ----------

(exposed_unexposed_buy_flag
 .withColumn("double_exp", F.count("mech_name").over(Window.partitionBy("household_id")))
 .where(F.col("double_exp")>1)
 .orderBy("household_id", "first_exposed_date", "first_shp_date")
).display()

# COMMAND ----------


