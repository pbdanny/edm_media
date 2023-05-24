import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval

spark = SparkSession.builder.appName("campaingEval").getOrCreate()

from utils import period_cal
from activate import activated

def create_txn_offline_x_aisle_matched_store(cmp: CampaignEval):
    
    cmp.aisle_matched_store = \
    (cmp
     .aisle_target_store_conf
     .join(cmp.matched_store.select(F.col("test_store_id").alias("store_id"), "ctrl_store_id").drop_duplicates(),
           "store_id"
           )
     .drop("store_id")
    )
    
    cmp.txn_offline_x_aisle_matched_store = \
        (cmp.txn.join(cmp.aisle_matched_store, ["store_id", "upc_id", "date_id"])
         .where(F.col("offline_online_other_channel")=="OFFLINE")
        )
        
    pass

#---- Unexposure by mechanics
def get_cust_all_unexposed_by_mech(cmp: CampaignEval):
    create_txn_offline_x_aisle_matched_store(cmp)
    cmp.cust_all_unexposed = \
        (cmp.txn_offline_x_aisle_matched_store
         .where(F.col("household_id").isNotNull())
         .select('household_id', 'transaction_uid', 'tran_datetime', 'mech_name', 'aisle_scope')
         .drop_duplicates()
         .withColumnRenamed('transaction_uid', 'unexposed_transaction_uid')
         .withColumnRenamed('tran_datetime', 'unexposed_tran_datetime')
        )
    pass

def get_cust_by_mech_last_seen_unexposed_tag(cmp: CampaignEval,
                                             prd_scope_df: SparkDataFrame,
                                             prd_scope_nm: str):
    """
    """
    get_cust_all_unexposed_by_mech(cmp)
    activated.get_cust_all_prod_purchase_date(cmp, prd_scope_df)

    txn_each_purchase_most_recent_media_unexposed = \
    (cmp.cust_all_unexposed
    .join(cmp.cust_all_prod_purchase, on='household_id', how='inner')
    .where(F.col('exposed_tran_datetime') <= F.col('purchase_tran_datetime'))
    .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('unexposed_tran_datetime'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('purchase_transaction_uid').orderBy(F.col('time_diff'))))
    .where(F.col('recency_rank') == 1).drop_duplicates()
    )
    purchased_unexposure_count = \
        (txn_each_purchase_most_recent_media_unexposed
        .groupBy('household_id', 'mech_name')
        .agg(F.count_distinct(F.col('purchase_transaction_uid')).alias("n_visit_purchased_unexposure"))
        )

    purchased_unexposure_n_mech = purchased_unexposure_count.select("mech_name").drop_duplicates().count()

    purchased_unexposure_flagged_pv = \
        (purchased_unexposure_n_mech
        .withColumn("exposure_flag", F.lit(1))
        .groupBy("household_id")
        .pivot("mech_name")
        .agg(F.first(F.col("unexposure_flag")))
        .fillna(0)
        )

    total_purchased_unexposure_flagged_by_cust = \
        (purchased_unexposure_flagged_pv
         .withColumn('level', F.lit(prd_scope_nm))
         .withColumn('total_mechanics_exposed',
            sum(purchased_unexposure_flagged_pv[col] for col in purchased_unexposure_flagged_pv.columns[1:purchased_unexposure_n_mech+1]))
         )
        
    return total_purchased_unexposure_flagged_by_cust

def get_customer_uplift(cmp: CampaignEval,
                        prd_scope_df: SparkDataFrame,
                        prd_scope_nm: str):
    """
    """
    #--- Helper fn
    def _get_period_wk_col_nm(wk_type: str
                              ) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo_week", "promo_wk"]:
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
        # Change filter column to offline_online_other_channel - Dec 2022 - Ta
        out = \
            (txn
             .where(F.col("offline_online_other_channel")==channel)
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
    
    period_wk_col = period_cal.get_period_wk_col_nm(cmp)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    
    activated = activated.get_cust_by_mech_last_seen_tag(cmp, prd_scope_df, prd_scope_nm)
    purchased = activated.get_cust_all_prod_purchase_date(cmp, prd_scope_df)
    
    
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