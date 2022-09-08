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

store_matching_df = pd.read_csv("/dbfs/FileStore/media/campaign_eval/01_hde/Jun_2022/2022_0012_M01M_Nescafe_Shelf_Divider/output/store_matching.csv")
ctr_store_list = list(set([s for s in store_matching_df.ctr_store_var]))

# COMMAND ----------

def get_customer_uplift(txn: SparkDataFrame,
                       cp_start_date: str,
                       cp_end_date: str,
                       wk_type: str,
                       test_store_sf: SparkDataFrame,
                       adj_prod_sf: SparkDataFrame,
                       brand_sf: SparkDataFrame,
                       feat_sf: SparkDataFrame,
                       ctr_store_list: List,
                       cust_uplift_lv: str):
    """DEV version
    Customer Uplift : Exposed vs Unexposed
    Exposed : shop adjacency product during campaing in test store
    Unexpose : shop adjacency product during campaing in control store
    In case customer exposed and unexposed -> flag customer as exposed
    """
    #--- Helper fn
    def _get_period_wk_col_nm(wk_type: str
                              ) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo_week"]:
            period_wk_col_nm = "period_promo_wk"
        elif wk_type in ["promozone"]:
            period_wk_col_nm = "period_promo_mv_wk"
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm

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
            )
        return filled_ctrl_store_sf

    def _create_adj_prod_df(txn: SparkDataFrame) -> SparkDataFrame:
        """If adj_prod_sf is None, create from all upc_id in txn
        """
        out = txn.select("upc_id").drop_duplicates().checkpoint()
        return out

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
             .join(test_store_sf, "store_id","inner") # Mapping cmp_start, cmp_end, mech_count, mech_name by store
             .join(adj_prod_sf, "upc_id", "inner")
             .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
             .groupBy("household_id")
             .agg(F.min("date_id").alias("first_exposed_date"))
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
             .groupBy('household_id')
             .agg(F.min('date_id').alias('first_shp_date'))
             .drop_duplicates()
            )
        return out

    def _get_mvmnt_prior_pre(txn: SparkDataFrame,
                             period_wk_col: str,
                             prd_scope_df: SparkDataFrame
                             ) -> SparkDataFrame:
        """Get customer movement prior (ppp) / pre (pre) of
        product scope
        """
        prior = \
            (txn
             .where(F.col(period_wk_col).isin(['ppp']))
             .where(F.col('household_id').isNotNull())
             .join(prd_scope_df, 'upc_id')
             .groupBy('household_id')
             .agg(F.sum('net_spend_amt').alias('prior_spending'))
             )
        pre = \
            (txn
             .where(F.col(period_wk_col).isin(['pre']))
             .where(F.col('household_id').isNotNull())
             .join(prd_scope_df, 'upc_id')
             .groupBy('household_id')
             .agg(F.sum('net_spend_amt').alias('pre_spending'))
             )
        prior_pre = prior.join(pre, "household_id", "outer").fillna(0)

        return prior_pre

    #---- Main
    print("-"*80)
    print("Customer Uplift")
    print("Media Exposed = shopped in media aisle within campaign period (base on target input file) at target store , channel OFFLINE ")
    print("Media UnExposed = shopped in media aisle within campaign period (base on target input file) at control store , channel OFFLINE ")
    print("-"*80)
    if adj_prod_sf is None:
        print("Media exposed use total store level (all products)")
        adj_prod_sf = _create_adj_prod_df(txn)
    print("-"*80)
    print(f"Activate = Exposed & Shop {cust_uplift_lv.upper()} in campaign period at any store format and any channel")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    if cust_uplift_lv == 'brand':
        prd_scope_df = brand_sf
    else:
        prd_scope_df = feat_sf

    ##---- Expose - UnExpose : Flag customer
    target_str = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
    cmp_exposed = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)

    ctr_str = _create_ctrl_store_sf(ctr_store_list=ctr_store_list, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
    cmp_unexposed = _get_exposed_cust(txn=txn, test_store_sf=ctr_str, adj_prod_sf=adj_prod_sf)

    exposed_flag = cmp_exposed.withColumn("exposed_flag", F.lit(1))
    unexposed_flag = cmp_unexposed.withColumn("unexposed_flag", F.lit(1)).withColumnRenamed("first_exposed_date", "first_unexposed_date")

    exposure_cust_table = exposed_flag.join(unexposed_flag, 'household_id', 'outer').fillna(0)

    ## Flag Shopper in campaign
    cmp_shppr = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=prd_scope_df)

    ## Combine flagged customer Exposed, UnExposed, Exposed-Buy, UnExposed-Buy
    exposed_unexposed_buy_flag = \
    (exposure_cust_table
     .join(cmp_shppr, 'household_id', 'left')
     .withColumn('exposed_and_buy_flag', F.when( (F.col('first_exposed_date').isNotNull() ) & \
                                                 (F.col('first_shp_date').isNotNull() ) & \
                                                 (F.col('first_exposed_date') <= F.col('first_shp_date')), '1').otherwise(0))
     .withColumn('unexposed_and_buy_flag', F.when( (F.col('first_exposed_date').isNull()) & \
                                                   (F.col('first_unexposed_date').isNotNull()) & \
                                                   (F.col('first_shp_date').isNotNull()) & \
                                                   (F.col('first_unexposed_date') <= F.col('first_shp_date')), '1').otherwise(0))
    )
    exposed_unexposed_buy_flag.groupBy('exposed_flag', 'unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag').count().show()

    ##---- Movement : prior - pre
    prior_pre = _get_mvmnt_prior_pre(txn=txn, period_wk_col=period_wk_col, prd_scope_df=prd_scope_df)

    ##---- Flag customer movement and exposure
    movement_and_exposure = \
    (exposed_unexposed_buy_flag
     .join(prior_pre,'household_id', 'left')
     .withColumn('customer_group',
                 F.when(F.col('pre_spending')>0,'existing')
                  .when(F.col('prior_spending')>0,'lapse')
                  .otherwise('new'))
    )

    movement_and_exposure.where(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.countDistinct('household_id')).show()

    ##---- Uplift Calculation
    ### Count customer by group
    n_cust_by_group = \
        (movement_and_exposure
         .groupby('customer_group','exposed_flag','unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag')
         .agg(F.countDistinct('household_id').alias('customers'))
        )
    gr_exposed = \
        (n_cust_by_group
         .where(F.col('exposed_flag')==1)
         .groupBy('customer_group')
         .agg(F.sum('customers').alias('exposed_customers'))
        )
    gr_exposed_buy = \
        (n_cust_by_group
         .where(F.col('exposed_and_buy_flag')==1)
         .groupBy('customer_group')
         .agg(F.sum('customers').alias('exposed_shoppers'))
         )
    gr_unexposed = \
        (n_cust_by_group
        .where( (F.col('exposed_flag')==0) & (F.col('unexposed_flag')==1) )
        .groupBy('customer_group').agg(F.sum('customers').alias('unexposed_customers'))
        )
    gr_unexposed_buy = \
        (n_cust_by_group
        .where(F.col('unexposed_and_buy_flag')==1)
        .groupBy('customer_group')
        .agg(F.sum('customers').alias('unexposed_shoppers'))
        )
    combine_gr = \
        (gr_exposed.join(gr_exposed_buy,'customer_group')
         .join(gr_unexposed,'customer_group')
         .join(gr_unexposed_buy,'customer_group')
        )

    ### Calculate conversion & uplift
    total_cust_uplift = (combine_gr
                         .agg(F.sum("exposed_customers").alias("exposed_customers"),
                              F.sum("exposed_shoppers").alias("exposed_shoppers"),
                              F.sum("unexposed_customers").alias("unexposed_customers"),
                              F.sum("unexposed_shoppers").alias("unexposed_shoppers")
                              )
                         .withColumn("customer_group", F.lit("Total"))
                        )

    uplift_w_total = combine_gr.unionByName(total_cust_uplift, allowMissingColumns=True)

    uplift_result = uplift_w_total.withColumn('uplift_lv', F.lit(cust_uplift_lv)) \
                          .withColumn('cvs_rate_test', F.col('exposed_shoppers')/F.col('exposed_customers'))\
                          .withColumn('cvs_rate_ctr', F.col('unexposed_shoppers')/F.col('unexposed_customers'))\
                          .withColumn('pct_uplift', F.col('cvs_rate_test')/F.col('cvs_rate_ctr') - 1 )\
                          .withColumn('uplift_cust',(F.col('cvs_rate_test')-F.col('cvs_rate_ctr'))*F.col('exposed_customers'))

    ### Re-calculation positive uplift & percent positive customer uplift
    positive_cust_uplift = \
        (uplift_result
         .where(F.col("customer_group")!="Total")
         .select("customer_group", "uplift_cust")
         .withColumn("pstv_cstmr_uplift", F.when(F.col("uplift_cust")>=0, F.col("uplift_cust")).otherwise(0))
         .select("customer_group", "pstv_cstmr_uplift")
        )
    total_positive_cust_uplift_num = positive_cust_uplift.agg(F.sum("pstv_cstmr_uplift")).collect()[0][0]
    total_positive_cust_uplift_sf = spark.createDataFrame([("Total", total_positive_cust_uplift_num),], ["customer_group", "pstv_cstmr_uplift"])
    recal_cust_uplift = positive_cust_uplift.unionByName(total_positive_cust_uplift_sf)

    uplift_out = \
        (uplift_result.join(recal_cust_uplift, "customer_group", "left")
         .withColumn("pct_positive_cust_uplift", F.col("pstv_cstmr_uplift")/F.col("exposed_shoppers"))
        )
    # Sort row order , export as SparkFrame
    df = uplift_out.toPandas()
    sort_dict = {"new":0, "existing":1, "lapse":2, "Total":3}
    df = df.sort_values(by=["customer_group"], key=lambda x: x.map(sort_dict))  # type: ignore
    uplift_out = spark.createDataFrame(df)

    return uplift_out

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
#  .where(F.col("proximity_rank")==1)
).display()

#  .orderBy(F.col("sec_diff").asc_nulls_last()).display()

# COMMAND ----------

(cmp_exposed_buy
 .where(F.col("household_id")==102111060002423872)
 .where(F.col("sec_diff")>=0)
 .withColumn("proximity_rank", 
             F.row_number().over(Window.partitionBy("household_id", "shp_txn_id")
                           .orderBy(F.col("sec_diff").asc_nulls_last())))
#  .where(F.col("proximity_rank")==1)
).display()

#  .orderBy(F.col("sec_diff").asc_nulls_last()).display()

# COMMAND ----------

(cmp_exposed_buy
 .where(F.col("household_id")==102111060002423872)
 .where(F.col("exposed_txn_id")==123515411042)
).display()

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

cmp_shppr = _get_shppr(txn=txn, period_wk_col_nm="period_fis_wk", prd_scope_df=brand_df)

## Combine flagged customer Exposed, UnExposed, Exposed-Buy, UnExposed-Buy
exposed_unexposed_buy_flag = \
(exposure_cust_table
.join(cmp_shppr, 'household_id', 'left')
.withColumn('exposed_and_buy_flag', F.when( (F.col('first_exposed_date').isNotNull() ) & \
                                           (F.col('first_shp_date').isNotNull() ) & \
                                           (F.col('first_exposed_date') <= F.col('first_shp_date')), '1').otherwise(0))
.withColumn('unexposed_and_buy_flag', F.when( (F.col('first_exposed_date').isNull()) & \
                                             (F.col('first_unexposed_date').isNotNull()) & \
                                             (F.col('first_shp_date').isNotNull()) & \
                                             (F.col('first_unexposed_date') <= F.col('first_shp_date')), '1').otherwise(0))
)

exposed_unexposed_buy_flag.display()
# exposed_unexposed_buy_flag.groupBy('exposed_flag', 'unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag').count().show()
exposed_unexposed_buy_flag.count()

# COMMAND ----------

(exposed_unexposed_buy_flag
 .withColumn("double_exp", F.count("mech_name").over(Window.partitionBy("household_id")))
 .where(F.col("double_exp")>1)
 .orderBy("household_id", "first_exposed_date", "first_shp_date")
).display()

# COMMAND ----------

adj_prod_sf = use_ai_df

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

# COMMAND ----------


