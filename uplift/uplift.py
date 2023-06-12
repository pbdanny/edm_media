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
from uplift import unexposed

def _get_cust_mvmnt_ppp_pre(cmp: CampaignEval,
                            prd_scope_df: SparkDataFrame,
                            prd_scope_nm: str
                            ) -> SparkDataFrame:
    """Get customer movement prior (ppp) / pre (pre) of
    product scope
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    prior = \
        (cmp.txn
         .where(F.col(period_wk_col_nm).isin(['ppp']))
         .where(F.col('household_id').isNotNull())
         .join(prd_scope_df, 'upc_id')
         .groupBy('household_id')
         .agg(F.sum('net_spend_amt').alias('prior_spending'))
         )
    pre = \
        (cmp.txn
         .where(F.col(period_wk_col_nm).isin(['pre']))
         .where(F.col('household_id').isNotNull())
         .join(prd_scope_df, 'upc_id')
         .groupBy('household_id')
         .agg(F.sum('net_spend_amt').alias('pre_spending'))
         )
    prior_pre = (prior
                 .join(pre, "household_id", "outer")
                 .fillna(0, subset=["prior_spending", "pre_spending"])
                 .withColumn('level', F.lit(prd_scope_nm))
                 .withColumn('customer_mv_group',
                 F.when(F.col('pre_spending')>0,'existing')
                  .when(F.col('prior_spending')>0,'lapse')
                  .otherwise('new'))
    )

    return prior_pre

def get_cust_uplift_any_mech(cmp: CampaignEval,
                             prd_scope_df: SparkDataFrame,
                             prd_scope_nm: str):
    """
    """

    #---- Main
    period_wk_col = period_cal.get_period_wk_col_nm(cmp)
    print(f"Customer movement period (PPP, PRE) based on column {period_wk_col}")
    
    cust_exposed = activated.get_cust_first_exposed_any_mech(cmp)
    cust_exposed_purchased = activated.get_cust_any_mech_activated(cmp, prd_scope_df, prd_scope_nm)
    cust_unexposed = unexposed.get_cust_first_unexposed_any_mech(cmp)
    cust_unexposed_purchased = unexposed.get_cust_any_mech_unexposed_purchased(cmp, prd_scope_df, prd_scope_nm)

    exposed_flag = cust_exposed.select("household_id").drop_duplicates().withColumn("exposed_flag", F.lit(1))
    unexposed_flag = cust_unexposed.select("household_id").drop_duplicates().withColumn("unexposed_flag", F.lit(1))
    exposed_x_unexposed_flag = exposed_flag.join(unexposed_flag, 'household_id', 'outer').fillna(0)
    exposed_purchased_flag = cust_exposed_purchased.select("household_id").drop_duplicates().withColumn('exposed_and_purchased_flag', F.lit(1))
    unexposed_purchased_flag = cust_unexposed_purchased.select("household_id").drop_duplicates().withColumn('unexposed_and_purchased_flag', F.lit(1))
    
    # Combine flagged customer Exposed, UnExposed, Exposed-Purchased, UnExposed-Purchased
    exposure_x_purchased_flag = \
    (exposed_x_unexposed_flag
     .join(exposed_purchased_flag, 'household_id', 'left')
     .join(unexposed_purchased_flag, 'household_id', 'left')
     .fillna(0, subset = ["exposed_and_purchased_flag", "unexposed_and_purchased_flag"])
    )
    
    exposure_x_purchased_flag.groupBy('exposed_flag', 'unexposed_flag','exposed_and_purchased_flag','unexposed_and_purchased_flag').count().display()

    #---- Movement : prior - pre
    cust_mv = _get_cust_mvmnt_ppp_pre(cmp, prd_scope_df, prd_scope_nm)
    cust_mv.groupBy('customer_mv_group').agg(F.countDistinct('household_id')).display()

    # Flag customer movement and exposure
    movement_x_exposure = \
    (exposure_x_purchased_flag
     .join(cust_mv.select("household_id", "customer_mv_group").drop_duplicates(), 'household_id', 'inner')
    )

    #---- Uplift Calculation
    # Count customer by group
    n_cust_by_group = \
        (movement_x_exposure
         .groupby('customer_mv_group','exposed_flag','unexposed_flag','exposed_and_purchased_flag','unexposed_and_purchased_flag')
         .agg(F.countDistinct('household_id').alias('customers'))
        )
    gr_exposed = \
        (n_cust_by_group
         .where(F.col('exposed_flag')==1)
         .groupBy('customer_mv_group')
         .agg(F.sum('customers').alias('exposed_customers'))
        )
    gr_exposed_buy = \
        (n_cust_by_group
         .where(F.col('exposed_and_purchased_flag')==1)
         .groupBy('customer_mv_group')
         .agg(F.sum('customers').alias('exposed_shoppers'))
         )
    gr_unexposed = \
        (n_cust_by_group
        .where( (F.col('exposed_flag')==0) & (F.col('unexposed_flag')==1) )
        .groupBy('customer_mv_group').agg(F.sum('customers').alias('unexposed_customers'))
        )
    gr_unexposed_buy = \
        (n_cust_by_group
        .where(F.col('unexposed_and_purchased_flag')==1)
        .groupBy('customer_mv_group')
        .agg(F.sum('customers').alias('unexposed_shoppers'))
        )
    combine_gr = \
        (gr_exposed.join(gr_exposed_buy,'customer_mv_group')
         .join(gr_unexposed,'customer_mv_group')
         .join(gr_unexposed_buy,'customer_mv_group')
        )

    ### Calculate conversion & uplift
    total_cust_uplift = (combine_gr
                         .agg(F.sum("exposed_customers").alias("exposed_customers"),
                              F.sum("exposed_shoppers").alias("exposed_shoppers"),
                              F.sum("unexposed_customers").alias("unexposed_customers"),
                              F.sum("unexposed_shoppers").alias("unexposed_shoppers")
                              )
                         .withColumn("customer_mv_group", F.lit("Total"))
                        )

    uplift_w_total = combine_gr.unionByName(total_cust_uplift, allowMissingColumns=True)

    uplift_result = uplift_w_total.withColumn('uplift_lv', F.lit(prd_scope_nm)) \
                          .withColumn('cvs_rate_test', F.col('exposed_shoppers')/F.col('exposed_customers'))\
                          .withColumn('cvs_rate_ctr', F.col('unexposed_shoppers')/F.col('unexposed_customers'))\
                          .withColumn('pct_uplift', F.col('cvs_rate_test')/F.col('cvs_rate_ctr') - 1 )\
                          .withColumn('uplift_cust',(F.col('cvs_rate_test')-F.col('cvs_rate_ctr'))*F.col('exposed_customers'))

    ### Re-calculation positive uplift & percent positive customer uplift
    positive_cust_uplift = \
        (uplift_result
         .where(F.col("customer_mv_group")!="Total")
         .select("customer_mv_group", "uplift_cust")
         .withColumn("pstv_cstmr_uplift", F.when(F.col("uplift_cust")>=0, F.col("uplift_cust")).otherwise(0))
         .select("customer_mv_group", "pstv_cstmr_uplift")
        )
    total_positive_cust_uplift_num = positive_cust_uplift.agg(F.sum("pstv_cstmr_uplift")).collect()[0][0]
    total_positive_cust_uplift_sf = spark.createDataFrame([("Total", total_positive_cust_uplift_num),], ["customer_mv_group", "pstv_cstmr_uplift"])
    recal_cust_uplift = positive_cust_uplift.unionByName(total_positive_cust_uplift_sf)

    uplift_out = \
        (uplift_result.join(recal_cust_uplift, "customer_mv_group", "left")
         .withColumn("pct_positive_cust_uplift", F.col("pstv_cstmr_uplift")/F.col("exposed_shoppers"))
        )
    # Sort row order , export as SparkFrame
    df = uplift_out.toPandas()
    sort_dict = {"new":0, "existing":1, "lapse":2, "Total":3}
    df = df.sort_values(by=["customer_mv_group"], key=lambda x: x.map(sort_dict))  # type: ignore
    uplift_out = spark.createDataFrame(df)

    return uplift_out

#---- Original code
def get_customer_uplift_per_mechanic(cmp: CampaignEval,
                                     prd_scope_df: SparkDataFrame,
                                     prd_scope_nm: str):
    
    """Customer Uplift : Exposed vs Unexposed
    Exposed : shop adjacency product during campaing in test store
    Unexpose : shop adjacency product during campaing in control store
    In case customer exposed and unexposed -> flag customer as exposed
    """
    txn = cmp.txn
    cp_start_date = cmp.cmp_start                                 
    cp_end_date = cmp.cmp_end
    wk_type = cmp.wk_type
    test_store_sf = cmp.target_store
    adj_prod_sf = cmp.aisle_sku
    brand_sf = cmp.feat_brand_sku
    feat_sf = cmp.feat_sku
    ctr_store_list = cmp.matched_store_list
    cust_uplift_lv = prd_scope_nm
    store_matching_df_var = cmp.matched_store
    
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
        
        Also replace special characters in mechanic names
        
        """
        filled_test_store_sf = \
            (test_store_sf
            .fillna(str(cp_start_date), subset='c_start')
            .fillna(str(cp_end_date), subset='c_end')
            .withColumn('mech_name', F.regexp_replace(F.col('mech_name'), "[^a-zA-Z0-9]", "_"))
            .drop_duplicates()
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
             .drop_duplicates()
            )
        return filled_ctrl_store_sf

    def _create_ctrl_store_sf_with_mech(filled_test_store_sf: SparkDataFrame,
                                        filled_ctrl_store_sf: SparkDataFrame,
                                        store_matching_df_var: SparkDataFrame,
                                        mechanic_list: List
                                        ) -> SparkDataFrame:
        '''
        Create control store table that tag the mechanic types of their matching test stores
        '''
        # For each mechanic, add boolean column tagging each test store
        store_matching_df_var_tagged = store_matching_df_var.join(filled_test_store_sf.select('store_id', 'mech_name').drop_duplicates(),
                                                                  on='store_id', how='left')

        for mech in mechanic_list:
            store_matching_df_var_tagged = store_matching_df_var_tagged.withColumn('flag_ctr_' + mech, F.when(F.col('mech_name') == mech, 1).otherwise(0))

        # Sum number of mechanics over each control store's window
        #windowSpec = Window.partitionBy('ctr_store_var')
        windowSpec = Window.partitionBy('ctr_store_cos') ## Change to crt_store_cos for matching type -- Feb 2028 Pat

        ctr_store_sum = store_matching_df_var_tagged.select("*")

        for mech in mechanic_list:
            ctr_store_sum = ctr_store_sum.withColumn('sum_ctr_' + mech, F.sum(F.col('flag_ctr_' + mech)).over(windowSpec)).drop('flag_ctr_' + mech)

        # Select control stores level only and drop dupes
        ctr_store_sum_only = ctr_store_sum.drop('store_id', 'mech_name').drop_duplicates()

        ctr_store_mech_flag = ctr_store_sum_only.select("*")

        # Turn into Boolean columns
        for mech in mechanic_list:
            ctr_store_mech_flag = ctr_store_mech_flag.withColumn('ctr_' + mech, F.when(F.col('sum_ctr_' + mech) > 0, 1).otherwise(0)).drop('sum_ctr_' + mech)
        
        #ctr_store_mech_flag = ctr_store_mech_flag.withColumnRenamed('ctr_store_var', 'store_id')  ## Change to crt_store_cos for matching type -- Feb 2028 Pat
        ctr_store_mech_flag = ctr_store_mech_flag.withColumnRenamed('ctr_store_cos', 'store_id')

        filled_ctrl_store_sf_with_mech = filled_ctrl_store_sf.join(ctr_store_mech_flag, on='store_id', how='left')

        filled_ctrl_store_sf_with_mech = filled_ctrl_store_sf_with_mech.drop('c_start', 'c_end')

        return filled_ctrl_store_sf_with_mech

    def _create_adj_prod_df(txn: SparkDataFrame) -> SparkDataFrame:
        """If adj_prod_sf is None, create from all upc_id in txn
        """
        out = txn.select("upc_id").drop_duplicates().checkpoint()
        return out

    def _get_all_feat_trans(txn: SparkDataFrame,
                            period_wk_col_nm: str,
                            prd_scope_df: SparkDataFrame
                            ) -> SparkDataFrame:
        """Get all shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel 
        """
        out = \
            (txn
             .where(F.col('household_id').isNotNull())
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(prd_scope_df, 'upc_id')
             .select('household_id', 'transaction_uid', 'tran_datetime', 'store_id', 'date_id')
             .drop_duplicates()
            )
        return out

    # Get the "last seen" mechanic(s) that a shopper saw before they make purchases
    def _get_activ_mech_last_seen(txn: SparkDataFrame,
                                  test_store_sf: SparkDataFrame,
                                  ctr_str: SparkDataFrame,
                                  adj_prod_sf: SparkDataFrame,
                                  period_wk_col: str,
                                  prd_scope_df: SparkDataFrame,
                                  cp_start_date: str,
                                  cp_end_date: str,
                                  filled_ctrl_store_sf_with_mech: SparkDataFrame
                                 ) -> SparkDataFrame:

        # Get all featured shopping transactions during campaign
        # Note: Left side does not filter out Online as customers exposed to media by buying Offline may end up buying product Online
        all_feat_trans_item_level = _get_all_feat_trans(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=prd_scope_df)

        all_feat_trans_trans_level = all_feat_trans_item_level.select('household_id', 'transaction_uid', 'tran_datetime', 'store_id', 'date_id') \
                                                              .filter(F.col('date_id').between(cp_start_date, cp_end_date)) \
                                                              .drop_duplicates()

        # Get txn that are only at stores with media and at aisles where media could be found, during time media could be found
        filled_test_store_sf = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)

        txn_test_store_media_aisles = (txn.where(F.col('household_id').isNotNull())
                                          .where(F.col(period_wk_col).isin(["cmp"]))
                                          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
                                          .join(filled_test_store_sf.select('store_id', 'c_start', 'c_end', 'mech_name'), on='store_id', how='inner')
                                          .join(adj_prod_sf.select('upc_id'), on='upc_id', how='inner')
                                          .filter(F.col('offline_online_other_channel') == 'OFFLINE')
                                          .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end')))
                                          .select('household_id', 'transaction_uid', 'tran_datetime', 'mech_name').drop_duplicates()
                                          .withColumnRenamed('transaction_uid', 'other_transaction_uid')
                                          .withColumnRenamed('tran_datetime', 'other_tran_datetime'))

        # For each featured shopping transaction, join with other media-exposed transactions of that customer,
        # and keep transactions only that happen at or prior to such transaction
        
        # Add cp_start_date cp_end_date filter
        # Edit Dec 2022 - changed from left to inner join
        txn_each_purchase = (all_feat_trans_trans_level.filter(F.col('date_id').between(cp_start_date, cp_end_date))
                                                       .join(txn_test_store_media_aisles, on='household_id', how='inner')
                                                       .filter(F.col('other_tran_datetime') <= F.col('tran_datetime')))

        # For each other transaction, get the difference between the time of that transaction and the featured shopping transaction
        # Rank (ascending) by the difference, and only take the other transaction with the lowest difference i.e. the most recent transaction
        windowSpec = Window.partitionBy('transaction_uid').orderBy(F.col('time_diff'))

        txn_each_purchase_rank = (txn_each_purchase.withColumn('time_diff', F.col('tran_datetime') - F.col('other_tran_datetime'))
                                                   .withColumn('recency_rank', F.dense_rank().over(windowSpec)))

        txn_each_purchase_most_recent_media_exposed = txn_each_purchase_rank.filter(F.col('recency_rank') == 1).drop_duplicates()

        # For each exposed featured product shopper, get the number of times they were exposed to each mechanic
        purchased_exposure_count = txn_each_purchase_most_recent_media_exposed.groupBy('household_id').pivot('mech_name').agg(F.countDistinct(F.col('transaction_uid'))).fillna(0)

        # For each mechanic, instead of count, change to flag (0 if no exposure, 1 if exposure regardless of count)
        mech_name_columns = purchased_exposure_count.columns
        mech_name_columns.remove('household_id')

        purchased_exposure_flagged = purchased_exposure_count.select("*")

        for col in mech_name_columns:
            purchased_exposure_flagged = purchased_exposure_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Find Non-exposed Purchased customers by getting transactions happening at Control stores, deducting any exposed customers found previously
        all_feat_trans_trans_level_control_store = all_feat_trans_trans_level.join(filled_ctrl_store_sf_with_mech, on='store_id', how='inner')

        all_purchased_exposed_shoppers = purchased_exposure_flagged.select('household_id').drop_duplicates()

        all_feat_trans_trans_level_control_store_nonexposed = all_feat_trans_trans_level_control_store.join(all_purchased_exposed_shoppers,
                                                                                                            on='household_id', how='leftanti')

        # For each customer, check from the control stores to see what mechanics are at the matching test stores
        ctr_mech_name_columns = filled_ctrl_store_sf_with_mech.columns
        ctr_mech_name_columns.remove('store_id')

        all_purchased_nonexposed_shoppers = all_feat_trans_trans_level_control_store_nonexposed.select('household_id').drop_duplicates()

        ctr_mech_count = {}

        for ctr_mech in ctr_mech_name_columns:
            mech = ctr_mech[4:]
            ctr_mech_count[ctr_mech] = all_feat_trans_trans_level_control_store_nonexposed.groupBy('household_id').agg(F.sum(F.col(ctr_mech)).alias(mech))

            all_purchased_nonexposed_shoppers = all_purchased_nonexposed_shoppers.join(ctr_mech_count[ctr_mech], on='household_id', how='left')

        # Convert to boolean
        purchased_nonexposed_shoppers_flagged = all_purchased_nonexposed_shoppers.select("*")

        for col in mech_name_columns:
            purchased_nonexposed_shoppers_flagged = purchased_nonexposed_shoppers_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Add Non-exposed Purchased to flagged list, filling all exposed flags with 0
        purchased_custs_flagged = purchased_exposure_flagged.withColumn('group', F.lit('Exposed_Purchased')) \
                                                            .unionByName(purchased_nonexposed_shoppers_flagged.withColumn('group', F.lit('Non_exposed_Purchased')))

        return purchased_custs_flagged

    def _get_non_shpprs_by_mech(txn: SparkDataFrame,
                                adj_prod_sf: SparkDataFrame,
                                cmp_shppr_last_seen: SparkDataFrame,
                                test_store_sf: SparkDataFrame,
                                ctr_str: SparkDataFrame,
                                cp_start_date: str,
                                cp_end_date: str,
                                period_wk_col: str,
                                filled_ctrl_store_sf_with_mech: SparkDataFrame
                               ) -> SparkDataFrame:

        # Get all adjacent transactions in test and control store - cover all exposed and non-exposed customers
        # For test stores, only have mechanic type if transaction happens within in the campaign period
        filled_test_store_sf = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)

        test_control_stores = filled_test_store_sf.unionByName(ctr_str, allowMissingColumns=True).fillna('1960-01-01', subset=['c_start', 'c_end'])

        txn_all_test_control_adj = (txn.where(F.col('household_id').isNotNull())
                                       .where(F.col(period_wk_col).isin(["cmp"]))
                                       .filter(F.col('date_id').between(cp_start_date, cp_end_date))
                                       .join(test_control_stores.select('store_id', 'c_start', 'c_end', 'mech_name'), on='store_id', how='inner')
                                       .join(adj_prod_sf.select('upc_id'), on='upc_id', how='inner')
                                       .filter(F.col('offline_online_other_channel') == 'OFFLINE')
                                       .select('household_id', 'transaction_uid', 'date_id', 'store_id', 'c_start', 'c_end', 'mech_name').drop_duplicates()
                                   )

        # Filter out all customers already identified as Purchased
        txn_non_purchased = txn_all_test_control_adj.join(cmp_shppr_last_seen.drop_duplicates(), on='household_id', how='leftanti')

        # For remaining Non-Purchased customers, group by and aggregate counts of how many times they have been exposed to each media
        # Only for transaction occuring in test stores during campaign period
        txn_non_purchased_test_dur = txn_non_purchased.filter(F.col('mech_name').isNotNull()).filter(F.col('date_id').between(F.col('c_start'), F.col('c_end')))
        nonpurchased_exposed_count = txn_non_purchased_test_dur.groupBy('household_id').pivot('mech_name').agg(F.countDistinct(F.col('transaction_uid'))).fillna(0)

        # For each mechanic, instead of count, change to flag (0 if no exposure, 1 if exposure regardless of count)
        mech_name_columns = nonpurchased_exposed_count.columns
        mech_name_columns.remove('household_id')

        nonpurchased_exposed_flagged = nonpurchased_exposed_count.select("*")

        for col in mech_name_columns:
            nonpurchased_exposed_flagged = nonpurchased_exposed_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Find Non-exposed Non-purchased customers by deducting exposed customers from the non-purchase adjacent visit customers list
        all_nonpurchased_exposed_shoppers = nonpurchased_exposed_flagged.select('household_id').drop_duplicates()

        all_nonpurchased_nonexposed_transactions = txn_non_purchased.join(all_nonpurchased_exposed_shoppers, on='household_id', how='leftanti')

        # Tag with Control store mech matching types
        all_nonpurchased_nonexposed_transactions_tagged = all_nonpurchased_nonexposed_transactions.join(filled_ctrl_store_sf_with_mech,
                                                                                                        on='store_id', how='left')

        # For each customer, check from the control stores to see what mechanics are at the matching test stores
        ctr_mech_name_columns = filled_ctrl_store_sf_with_mech.columns
        ctr_mech_name_columns.remove('store_id')

        all_nonpurchased_nonexposed_shoppers = all_nonpurchased_nonexposed_transactions_tagged.select('household_id').drop_duplicates()

        ctr_mech_count = {}

        for ctr_mech in ctr_mech_name_columns:
            mech = ctr_mech[4:]
            ctr_mech_count[ctr_mech] = all_nonpurchased_nonexposed_transactions_tagged.groupBy('household_id').agg(F.sum(F.col(ctr_mech)).alias(mech))

            all_nonpurchased_nonexposed_shoppers = all_nonpurchased_nonexposed_shoppers.join(ctr_mech_count[ctr_mech], on='household_id', how='left')

        # Convert to boolean
        nonpurchased_nonexposed_shoppers_flagged = all_nonpurchased_nonexposed_shoppers.select("*")

        for col in mech_name_columns:
            nonpurchased_nonexposed_shoppers_flagged = nonpurchased_nonexposed_shoppers_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Add Non-exposed Non-purchased to flagged list, filling all exposed flags with 0
        nonpurchased_custs_flagged = nonpurchased_exposed_flagged.withColumn('group', F.lit('Exposed_Non_purchased')) \
                                                                 .unionByName(nonpurchased_nonexposed_shoppers_flagged.withColumn('group', F.lit('Non_exposed_Non_purchased')))

        return nonpurchased_custs_flagged


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

    def _get_total_cust_per_mech(n_cust_total: SparkDataFrame,
                                 ex_pur_group: str,
                                 movement_and_exposure_by_mech: SparkDataFrame,
                                 mechanic_list: List
                                ) -> SparkDataFrame:
        '''Get total numbers of customers, divided into group New/Existing/Lapse
        '''

        n_cust_mech = {}

        # Get numbers of customers per each mechanic
        for mech in mechanic_list:
            # Get number of customers per each customer type (new/existing/lapse)
            n_cust_mech[mech] = movement_and_exposure_by_mech.filter(F.col('group') == ex_pur_group).filter(F.col(mech) == 1) \
                                                             .groupBy('customer_group') \
                                                             .agg(F.countDistinct(F.col('household_id')).alias(ex_pur_group + '_' + mech)) \
                                                             .fillna(0)

            # Also get total column for all 3 types
            n_cust_mech[mech] = n_cust_mech[mech].unionByName(n_cust_mech[mech] \
                                                 .agg(F.sum(F.col(ex_pur_group + '_' + mech)) \
                                                       .alias(ex_pur_group + '_' + mech)).fillna(0) \
                                                 .withColumn('customer_group', F.lit('Total')))
            
            n_cust_total = n_cust_total.join(n_cust_mech[mech].select('customer_group', ex_pur_group + '_' + mech), on='customer_group', how='left')

        return n_cust_total

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
#     cmp_exposed = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)

    mechanic_list = target_str.select('mech_name').drop_duplicates().rdd.flatMap(lambda x: x).collect()

    print("List of detected mechanics from store list: ", mechanic_list)

    ctr_str = _create_ctrl_store_sf(ctr_store_list=ctr_store_list, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
#     cmp_unexposed = _get_exposed_cust(txn=txn, test_store_sf=ctr_str, adj_prod_sf=adj_prod_sf)

    filled_ctrl_store_sf_with_mech = _create_ctrl_store_sf_with_mech(filled_test_store_sf=target_str,
                                                                     filled_ctrl_store_sf=ctr_str,
                                                                     store_matching_df_var=store_matching_df_var,
                                                                     mechanic_list=mechanic_list)

    ## Tag exposed media of each shopper
    cmp_shppr_last_seen = _get_activ_mech_last_seen(txn=txn, test_store_sf=target_str, ctr_str=ctr_str, adj_prod_sf=adj_prod_sf,
                                                    period_wk_col=period_wk_col, prd_scope_df=prd_scope_df,
                                                    cp_start_date=cp_start_date, cp_end_date=cp_end_date,
                                                    filled_ctrl_store_sf_with_mech=filled_ctrl_store_sf_with_mech)

    ## Find non-shoppers who are exposed and unexposed
    non_cmp_shppr_exposure = _get_non_shpprs_by_mech(txn=txn, adj_prod_sf=adj_prod_sf, cmp_shppr_last_seen=cmp_shppr_last_seen, test_store_sf=target_str, ctr_str=ctr_str,
                                                     cp_start_date=cp_start_date, cp_end_date=cp_end_date, period_wk_col=period_wk_col,
                                                     filled_ctrl_store_sf_with_mech=filled_ctrl_store_sf_with_mech)

    ## Tag each customer by group for shopper group
    ## If no exposure flag in any mechanic, then Non-exposed Purchased
    ## If exposure in any mechanic, then Exposed Purchased
    num_of_mechanics = len(mechanic_list)

    cmp_shppr_last_seen_tag = cmp_shppr_last_seen.withColumn('total_mechanics_exposed',
                                                             np.sum(cmp_shppr_last_seen[col] for col in cmp_shppr_last_seen.columns[1:num_of_mechanics+1]))

    ## Tag each customer by group for non-shopper group
    ## If no exposure flag in any mechanic, then Non-exposed Non-purchased
    ## If exposure in any mechanic, then Exposed Non-purchased
    non_cmp_shppr_exposure_tag = non_cmp_shppr_exposure.withColumn('total_mechanics_exposed',
                                                                   np.sum(non_cmp_shppr_exposure[col] for col in non_cmp_shppr_exposure.columns[1:num_of_mechanics+1]))
    
    # Add the two lists together
    exposed_unexposed_buy_flag_by_mech = cmp_shppr_last_seen_tag.unionByName(non_cmp_shppr_exposure_tag)

    # Show summary in cell output
    print('exposure groups new logic:')
    exposed_unexposed_buy_flag_by_mech.groupBy('group').pivot('total_mechanics_exposed').agg(F.countDistinct(F.col('household_id'))).fillna(0).show()

    ##---- Movement : prior - pre
    prior_pre = _get_mvmnt_prior_pre(txn=txn, period_wk_col=period_wk_col, prd_scope_df=prd_scope_df)

    ##---- Flag customer movement and exposure
    movement_and_exposure_by_mech = \
    (exposed_unexposed_buy_flag_by_mech
     .join(prior_pre,'household_id', 'left')
     .withColumn('customer_group',
                 F.when(F.col('pre_spending')>0,'existing')
                  .when(F.col('prior_spending')>0,'lapse')
                  .otherwise('new'))
    )
    
    username_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace('.', '').replace('@', '')

    # Save and load temp table
    spark.sql('DROP TABLE IF EXISTS tdm_seg.cust_uplift_by_mech_temp' + username_str)
    movement_and_exposure_by_mech.write.saveAsTable('tdm_seg.cust_uplift_by_mech_temp' + username_str)

    movement_and_exposure_by_mech = spark.table('tdm_seg.cust_uplift_by_mech_temp' + username_str)

    print('customer movement new logic:')
    movement_and_exposure_by_mech.groupBy('customer_group').pivot('group').agg(F.countDistinct('household_id')).show()


    ##---- Uplift Calculation by mechanic

    # Total customers for each exposure tag (Non-exposed Purchased, Non-exposed Non-purchased, Exposed Purchased, Exposed Non-purchased)
    n_cust_total_non_exposed_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Purchased') \
                                                                      .groupBy('customer_group') \
                                                                      .agg(F.countDistinct(F.col('household_id')).alias('Non_exposed_Purchased_all')) \
                                                                      .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Purchased') \
                                                                                   .agg(F.countDistinct(F.col('household_id')) \
                                                                                         .alias('Non_exposed_Purchased_all')).fillna(0) \
                                                                                   .withColumn('customer_group', F.lit('Total')))

    n_cust_total_non_exposed_non_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Non_purchased') \
                                                                          .groupBy('customer_group') \
                                                                          .agg(F.countDistinct(F.col('household_id')).alias('Non_exposed_Non_purchased_all')) \
                                                                          .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Non_purchased') \
                                                                                       .agg(F.countDistinct(F.col('household_id')) \
                                                                                             .alias('Non_exposed_Non_purchased_all')).fillna(0) \
                                                                                       .withColumn('customer_group', F.lit('Total')))


    n_cust_total_exposed_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Purchased') \
                                                                  .groupBy('customer_group') \
                                                                  .agg(F.countDistinct(F.col('household_id')).alias('Exposed_Purchased_all')) \
                                                                  .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Purchased') \
                                                                               .agg(F.countDistinct(F.col('household_id')) \
                                                                                     .alias('Exposed_Purchased_all')).fillna(0) \
                                                                               .withColumn('customer_group', F.lit('Total')))



    n_cust_total_exposed_non_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Non_purchased') \
                                                                      .groupBy('customer_group') \
                                                                      .agg(F.countDistinct(F.col('household_id')).alias('Exposed_Non_purchased_all'))\
                                                                      .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Non_purchased') \
                                                                                   .agg(F.countDistinct(F.col('household_id')) \
                                                                                         .alias('Exposed_Non_purchased_all')).fillna(0) \
                                                                                   .withColumn('customer_group', F.lit('Total')))



    # Total customers for Exposed Purchased and Exposed Non-purchased per each mechanic (if more than 1 mechanic)
    if num_of_mechanics > 1:

        n_cust_total_exposed_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_exposed_purchased,
                                                                ex_pur_group='Exposed_Purchased',
                                                                movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                mechanic_list=mechanic_list)

        n_cust_total_exposed_non_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_exposed_non_purchased,
                                                                    ex_pur_group='Exposed_Non_purchased',
                                                                    movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                    mechanic_list=mechanic_list)

        n_cust_total_non_exposed_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_non_exposed_purchased,
                                                                    ex_pur_group='Non_exposed_Purchased',
                                                                    movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                    mechanic_list=mechanic_list)

        n_cust_total_non_exposed_non_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_non_exposed_non_purchased,
                                                                        ex_pur_group='Non_exposed_Non_purchased',
                                                                        movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                        mechanic_list=mechanic_list)


    combine_n_cust = n_cust_total_non_exposed_purchased.join(n_cust_total_non_exposed_non_purchased, on='customer_group', how='left') \
                                                       .join(n_cust_total_exposed_purchased, on='customer_group', how='left') \
                                                       .join(n_cust_total_exposed_non_purchased, on='customer_group', how='left')

#     combine_n_cust.show()


    ## Conversion and Uplift New Logic
    # Get basic calcuations of conversion rates, uplift percent and number of customers
    results = combine_n_cust.withColumn('uplift_lv', F.lit(cust_uplift_lv)) \
                            .withColumn('mechanic', F.lit('all')) \
                            .withColumn('num_exposed_buy', F.col('Exposed_Purchased_all')) \
                            .withColumn('num_exposed_not_buy', F.col('Exposed_Non_purchased_all')) \
                            .withColumn('num_unexposed_buy', F.col('Non_exposed_Purchased_all')) \
                            .withColumn('num_unexposed_not_buy', F.col('Non_exposed_Non_purchased_all')) \
                            .withColumn('cvs_rate_exposed',
                                        F.col('Exposed_Purchased_all') / (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all'))) \
                            .withColumn('cvs_rate_unexposed',
                                        F.col('Non_exposed_Purchased_all') / (F.col('Non_exposed_Purchased_all') + F.col('Non_exposed_Non_purchased_all'))) \
                            .withColumn('pct_uplift',
                                        (F.col('cvs_rate_exposed') / (F.col('cvs_rate_unexposed'))) - 1) \
                            .withColumn('uplift_cust',
                                        (F.col('cvs_rate_exposed') - F.col('cvs_rate_unexposed')) *
                                        (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all')))

    # Get only positive customer uplift for each customer group (New/Lapse/Existing)
    pstv_cstmr_uplift_all_mech_col = results.select('customer_group', 'uplift_cust').filter("customer_group <> 'Total'") \
                                            .withColumn('pstv_cstmr_uplift',
                                                        F.when(F.col('uplift_cust') > 0, F.col('uplift_cust')).otherwise(0))

    # Get Total customer uplift, ignoring negative values
    pstv_cstmr_uplift_all_mech_col = pstv_cstmr_uplift_all_mech_col.select('customer_group', 'pstv_cstmr_uplift') \
                                                                   .unionByName(pstv_cstmr_uplift_all_mech_col.agg(F.sum(F.col('pstv_cstmr_uplift')) \
                                                                                                                    .alias('pstv_cstmr_uplift')).fillna(0) \
                                                                                                              .withColumn('customer_group', F.lit('Total')))

    results = results.join(pstv_cstmr_uplift_all_mech_col.select('customer_group', 'pstv_cstmr_uplift'), on='customer_group', how='left')

    # Recalculate uplift using total positive customers
    results = results.withColumn('pct_positive_cust_uplift',
                                 (F.col('pstv_cstmr_uplift') / (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all'))) / F.col('cvs_rate_unexposed'))
    
    # Sort row order , export as SparkFrame
    df = results.select('customer_group',
                        'mechanic',
                        'num_exposed_buy',
                        'num_exposed_not_buy',
                        'num_unexposed_buy',
                        'num_unexposed_not_buy',
                        'cvs_rate_exposed',
                        'cvs_rate_unexposed',
                        'pct_uplift',
                        'uplift_cust',
                        'pstv_cstmr_uplift',
                        'pct_positive_cust_uplift').toPandas()
    sort_dict = {"new":0, "existing":1, "lapse":2, "Total":3}
    df = df.sort_values(by=["customer_group"], key=lambda x: x.map(sort_dict))  # type: ignore
    results = spark.createDataFrame(df)

    # Repeat for all mechanics if multiple mechanics
    if num_of_mechanics > 1:
        mech_result = {}
        pstv_cstmr_uplift_col = {}

        for mech in mechanic_list:
            mech_result[mech] = combine_n_cust.withColumn('mechanic', F.lit(mech)) \
                                              .withColumn('num_exposed_buy', F.col('Exposed_Purchased_' + mech)) \
                                              .withColumn('num_exposed_not_buy', F.col('Exposed_Non_purchased_' + mech)) \
                                              .withColumn('num_unexposed_buy', F.col('Non_exposed_Purchased_' + mech)) \
                                              .withColumn('num_unexposed_not_buy', F.col('Non_exposed_Non_purchased_' + mech)) \
                                              .withColumn('cvs_rate_exposed',
                                                          F.col('Exposed_Purchased_' + mech) /
                                                          (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech))) \
                                              .withColumn('cvs_rate_unexposed',
                                                          F.col('Non_exposed_Purchased_' + mech) / (F.col('Non_exposed_Purchased_' + mech) + F.col('Non_exposed_Non_purchased_' + mech))) \
                                              .withColumn('pct_uplift',
                                                          (F.col('cvs_rate_exposed') / (F.col('cvs_rate_unexposed'))) - 1) \
                                              .withColumn('uplift_cust',
                                                          (F.col('cvs_rate_exposed') - F.col('cvs_rate_unexposed')) *
                                                          (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech)))

            pstv_cstmr_uplift_col[mech] = mech_result[mech].select('customer_group', 'uplift_cust').filter("customer_group <> 'Total'") \
                                                           .withColumn('pstv_cstmr_uplift',
                                                                       F.when(F.col('uplift_cust') > 0, F.col('uplift_cust')).otherwise(0))

            pstv_cstmr_uplift_col[mech] = pstv_cstmr_uplift_col[mech].select('customer_group', 'pstv_cstmr_uplift') \
                                                                     .unionByName(pstv_cstmr_uplift_col[mech].agg(F.sum(F.col('pstv_cstmr_uplift')) \
                                                                                                                   .alias('pstv_cstmr_uplift')).fillna(0) \
                                                                                                             .withColumn('customer_group', F.lit('Total')))

            mech_result[mech] = mech_result[mech].join(pstv_cstmr_uplift_col[mech].select('customer_group', 'pstv_cstmr_uplift'), on='customer_group', how='left')

            mech_result[mech] = mech_result[mech].withColumn('pct_positive_cust_uplift',
                                                             (F.col('pstv_cstmr_uplift') /
                                                              (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech))) /
                                                             F.col('cvs_rate_unexposed'))
            
            # Sort row order , export as SparkFrame
            df = mech_result[mech].toPandas()
            sort_dict = {"new":0, "existing":1, "lapse":2, "Total":3}
            df = df.sort_values(by=["customer_group"], key=lambda x: x.map(sort_dict))  # type: ignore
            mech_result[mech] = spark.createDataFrame(df)

            results = results.unionByName(mech_result[mech].select('customer_group',
                                                                   'mechanic',
                                                                   'num_exposed_buy',
                                                                   'num_exposed_not_buy',
                                                                   'num_unexposed_buy',
                                                                   'num_unexposed_not_buy',
                                                                   'cvs_rate_exposed',
                                                                   'cvs_rate_unexposed',
                                                                   'pct_uplift',
                                                                   'uplift_cust',
                                                                   'pstv_cstmr_uplift',
                                                                   'pct_positive_cust_uplift'))

    return results, movement_and_exposure_by_mech

#---- New code
def get_cust_uplift_by_mech(cmp: CampaignEval,
                            prd_scope_df: SparkDataFrame,
                            prd_scope_nm: str):
    
    #--- Helper fn

    def _get_all_feat_trans(txn: SparkDataFrame,
                            period_wk_col_nm: str,
                            prd_scope_df: SparkDataFrame
                            ) -> SparkDataFrame:
        """Get all shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel 
        """
        out = \
            (txn
             .where(F.col('household_id').isNotNull())
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(prd_scope_df, 'upc_id')
             .select('household_id', 'transaction_uid', 'tran_datetime', 'store_id', 'date_id')
             .drop_duplicates()
            )
        return out

    # Get the "last seen" mechanic(s) that a shopper saw before they make purchases
    def _get_activ_mech_last_seen(txn: SparkDataFrame,
                                  test_store_sf: SparkDataFrame,
                                  ctr_str: SparkDataFrame,
                                  adj_prod_sf: SparkDataFrame,
                                  period_wk_col: str,
                                  prd_scope_df: SparkDataFrame,
                                  cp_start_date: str,
                                  cp_end_date: str,
                                  filled_ctrl_store_sf_with_mech: SparkDataFrame
                                 ) -> SparkDataFrame:

        # Get all featured shopping transactions during campaign
        # Note: Left side does not filter out Online as customers exposed to media by buying Offline may end up buying product Online
        all_feat_trans_item_level = _get_all_feat_trans(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=prd_scope_df)

        all_feat_trans_trans_level = all_feat_trans_item_level.select('household_id', 'transaction_uid', 'tran_datetime', 'store_id', 'date_id') \
                                                              .filter(F.col('date_id').between(cp_start_date, cp_end_date)) \
                                                              .drop_duplicates()

        # Get txn that are only at stores with media and at aisles where media could be found, during time media could be found
        filled_test_store_sf = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)

        txn_test_store_media_aisles = (txn.where(F.col('household_id').isNotNull())
                                          .where(F.col(period_wk_col).isin(["cmp"]))
                                          .filter(F.col('date_id').between(cp_start_date, cp_end_date))
                                          .join(filled_test_store_sf.select('store_id', 'c_start', 'c_end', 'mech_name'), on='store_id', how='inner')
                                          .join(adj_prod_sf.select('upc_id'), on='upc_id', how='inner')
                                          .filter(F.col('offline_online_other_channel') == 'OFFLINE')
                                          .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end')))
                                          .select('household_id', 'transaction_uid', 'tran_datetime', 'mech_name').drop_duplicates()
                                          .withColumnRenamed('transaction_uid', 'other_transaction_uid')
                                          .withColumnRenamed('tran_datetime', 'other_tran_datetime'))

        # For each featured shopping transaction, join with other media-exposed transactions of that customer,
        # and keep transactions only that happen at or prior to such transaction
        
        # Add cp_start_date cp_end_date filter
        # Edit Dec 2022 - changed from left to inner join
        txn_each_purchase = (all_feat_trans_trans_level.filter(F.col('date_id').between(cp_start_date, cp_end_date))
                                                       .join(txn_test_store_media_aisles, on='household_id', how='inner')
                                                       .filter(F.col('other_tran_datetime') <= F.col('tran_datetime')))

        # For each other transaction, get the difference between the time of that transaction and the featured shopping transaction
        # Rank (ascending) by the difference, and only take the other transaction with the lowest difference i.e. the most recent transaction
        windowSpec = Window.partitionBy('transaction_uid').orderBy(F.col('time_diff'))

        txn_each_purchase_rank = (txn_each_purchase.withColumn('time_diff', F.col('tran_datetime') - F.col('other_tran_datetime'))
                                                   .withColumn('recency_rank', F.dense_rank().over(windowSpec)))

        txn_each_purchase_most_recent_media_exposed = txn_each_purchase_rank.filter(F.col('recency_rank') == 1).drop_duplicates()

        # For each exposed featured product shopper, get the number of times they were exposed to each mechanic
        purchased_exposure_count = txn_each_purchase_most_recent_media_exposed.groupBy('household_id').pivot('mech_name').agg(F.countDistinct(F.col('transaction_uid'))).fillna(0)

        # For each mechanic, instead of count, change to flag (0 if no exposure, 1 if exposure regardless of count)
        mech_name_columns = purchased_exposure_count.columns
        mech_name_columns.remove('household_id')

        purchased_exposure_flagged = purchased_exposure_count.select("*")

        for col in mech_name_columns:
            purchased_exposure_flagged = purchased_exposure_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Find Non-exposed Purchased customers by getting transactions happening at Control stores, deducting any exposed customers found previously
        all_feat_trans_trans_level_control_store = all_feat_trans_trans_level.join(filled_ctrl_store_sf_with_mech, on='store_id', how='inner')

        all_purchased_exposed_shoppers = purchased_exposure_flagged.select('household_id').drop_duplicates()

        all_feat_trans_trans_level_control_store_nonexposed = all_feat_trans_trans_level_control_store.join(all_purchased_exposed_shoppers,
                                                                                                            on='household_id', how='leftanti')

        # For each customer, check from the control stores to see what mechanics are at the matching test stores
        ctr_mech_name_columns = filled_ctrl_store_sf_with_mech.columns
        ctr_mech_name_columns.remove('store_id')

        all_purchased_nonexposed_shoppers = all_feat_trans_trans_level_control_store_nonexposed.select('household_id').drop_duplicates()

        ctr_mech_count = {}

        for ctr_mech in ctr_mech_name_columns:
            mech = ctr_mech[4:]
            ctr_mech_count[ctr_mech] = all_feat_trans_trans_level_control_store_nonexposed.groupBy('household_id').agg(F.sum(F.col(ctr_mech)).alias(mech))

            all_purchased_nonexposed_shoppers = all_purchased_nonexposed_shoppers.join(ctr_mech_count[ctr_mech], on='household_id', how='left')

        # Convert to boolean
        purchased_nonexposed_shoppers_flagged = all_purchased_nonexposed_shoppers.select("*")

        for col in mech_name_columns:
            purchased_nonexposed_shoppers_flagged = purchased_nonexposed_shoppers_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Add Non-exposed Purchased to flagged list, filling all exposed flags with 0
        purchased_custs_flagged = purchased_exposure_flagged.withColumn('group', F.lit('Exposed_Purchased')) \
                                                            .unionByName(purchased_nonexposed_shoppers_flagged.withColumn('group', F.lit('Non_exposed_Purchased')))

        return purchased_custs_flagged

    def _get_non_shpprs_by_mech(txn: SparkDataFrame,
                                adj_prod_sf: SparkDataFrame,
                                cmp_shppr_last_seen: SparkDataFrame,
                                test_store_sf: SparkDataFrame,
                                ctr_str: SparkDataFrame,
                                cp_start_date: str,
                                cp_end_date: str,
                                period_wk_col: str,
                                filled_ctrl_store_sf_with_mech: SparkDataFrame
                               ) -> SparkDataFrame:

        # Get all adjacent transactions in test and control store - cover all exposed and non-exposed customers
        # For test stores, only have mechanic type if transaction happens within in the campaign period
        filled_test_store_sf = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)

        test_control_stores = filled_test_store_sf.unionByName(ctr_str, allowMissingColumns=True).fillna('1960-01-01', subset=['c_start', 'c_end'])

        txn_all_test_control_adj = (txn.where(F.col('household_id').isNotNull())
                                       .where(F.col(period_wk_col).isin(["cmp"]))
                                       .filter(F.col('date_id').between(cp_start_date, cp_end_date))
                                       .join(test_control_stores.select('store_id', 'c_start', 'c_end', 'mech_name'), on='store_id', how='inner')
                                       .join(adj_prod_sf.select('upc_id'), on='upc_id', how='inner')
                                       .filter(F.col('offline_online_other_channel') == 'OFFLINE')
                                       .select('household_id', 'transaction_uid', 'date_id', 'store_id', 'c_start', 'c_end', 'mech_name').drop_duplicates()
                                   )

        # Filter out all customers already identified as Purchased
        txn_non_purchased = txn_all_test_control_adj.join(cmp_shppr_last_seen.drop_duplicates(), on='household_id', how='leftanti')

        # For remaining Non-Purchased customers, group by and aggregate counts of how many times they have been exposed to each media
        # Only for transaction occuring in test stores during campaign period
        txn_non_purchased_test_dur = txn_non_purchased.filter(F.col('mech_name').isNotNull()).filter(F.col('date_id').between(F.col('c_start'), F.col('c_end')))
        nonpurchased_exposed_count = txn_non_purchased_test_dur.groupBy('household_id').pivot('mech_name').agg(F.countDistinct(F.col('transaction_uid'))).fillna(0)

        # For each mechanic, instead of count, change to flag (0 if no exposure, 1 if exposure regardless of count)
        mech_name_columns = nonpurchased_exposed_count.columns
        mech_name_columns.remove('household_id')

        nonpurchased_exposed_flagged = nonpurchased_exposed_count.select("*")

        for col in mech_name_columns:
            nonpurchased_exposed_flagged = nonpurchased_exposed_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Find Non-exposed Non-purchased customers by deducting exposed customers from the non-purchase adjacent visit customers list
        all_nonpurchased_exposed_shoppers = nonpurchased_exposed_flagged.select('household_id').drop_duplicates()

        all_nonpurchased_nonexposed_transactions = txn_non_purchased.join(all_nonpurchased_exposed_shoppers, on='household_id', how='leftanti')

        # Tag with Control store mech matching types
        all_nonpurchased_nonexposed_transactions_tagged = all_nonpurchased_nonexposed_transactions.join(filled_ctrl_store_sf_with_mech,
                                                                                                        on='store_id', how='left')

        # For each customer, check from the control stores to see what mechanics are at the matching test stores
        ctr_mech_name_columns = filled_ctrl_store_sf_with_mech.columns
        ctr_mech_name_columns.remove('store_id')

        all_nonpurchased_nonexposed_shoppers = all_nonpurchased_nonexposed_transactions_tagged.select('household_id').drop_duplicates()

        ctr_mech_count = {}

        for ctr_mech in ctr_mech_name_columns:
            mech = ctr_mech[4:]
            ctr_mech_count[ctr_mech] = all_nonpurchased_nonexposed_transactions_tagged.groupBy('household_id').agg(F.sum(F.col(ctr_mech)).alias(mech))

            all_nonpurchased_nonexposed_shoppers = all_nonpurchased_nonexposed_shoppers.join(ctr_mech_count[ctr_mech], on='household_id', how='left')

        # Convert to boolean
        nonpurchased_nonexposed_shoppers_flagged = all_nonpurchased_nonexposed_shoppers.select("*")

        for col in mech_name_columns:
            nonpurchased_nonexposed_shoppers_flagged = nonpurchased_nonexposed_shoppers_flagged.withColumn(col, F.when(F.col(col) == 0, 0).otherwise(1))

        # Add Non-exposed Non-purchased to flagged list, filling all exposed flags with 0
        nonpurchased_custs_flagged = nonpurchased_exposed_flagged.withColumn('group', F.lit('Exposed_Non_purchased')) \
                                                                 .unionByName(nonpurchased_nonexposed_shoppers_flagged.withColumn('group', F.lit('Non_exposed_Non_purchased')))

        return nonpurchased_custs_flagged

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

    def _get_total_cust_per_mech(n_cust_total: SparkDataFrame,
                                 ex_pur_group: str,
                                 movement_and_exposure_by_mech: SparkDataFrame,
                                 mechanic_list: List
                                ) -> SparkDataFrame:
        '''Get total numbers of customers, divided into group New/Existing/Lapse
        '''

        n_cust_mech = {}

        # Get numbers of customers per each mechanic
        for mech in mechanic_list:
            # Get number of customers per each customer type (new/existing/lapse)
            n_cust_mech[mech] = movement_and_exposure_by_mech.filter(F.col('group') == ex_pur_group).filter(F.col(mech) == 1) \
                                                             .groupBy('customer_mv_group') \
                                                             .agg(F.countDistinct(F.col('household_id')).alias(ex_pur_group + '_' + mech)) \
                                                             .fillna(0)

            # Also get total column for all 3 types
            n_cust_mech[mech] = n_cust_mech[mech].unionByName(n_cust_mech[mech] \
                                                 .agg(F.sum(F.col(ex_pur_group + '_' + mech)) \
                                                       .alias(ex_pur_group + '_' + mech)).fillna(0) \
                                                 .withColumn('customer_mv_group', F.lit('Total')))
            
            n_cust_total = n_cust_total.join(n_cust_mech[mech].select('customer_mv_group', ex_pur_group + '_' + mech), on='customer_mv_group', how='left')

        return n_cust_total

    #---- Main
    period_wk_col = period_cal.get_period_wk_col_nm(cmp)
    print(f"Customer movement period (PPP, PRE) based on column {period_wk_col}")

    mechanic_list = cmp.target_store.select('mech_name').drop_duplicates().rdd.flatMap(lambda x: x).collect()
    print("List of detected mechanics from store list: ", mechanic_list)
    num_of_mechanics = len(mechanic_list)

    # Exposed
    cust_exposed_by_mech = activated.get_cust_txn_all_exposed_date_n_mech(cmp).select("household_id", "mech_name").drop_duplicates()
    cust_purchased_exposed = activated.get_cust_by_mech_purchased_exposed(cmp, prd_scope_df, prd_scope_nm).drop_duplicates()
    
    # Unexposed
    cust_unexposed_by_mech = unexposed.get_cust_txn_all_unexposed_date_n_mech(cmp).select("household_id", "mech_name").drop_duplicates()
    cust_purchased_unexposed = activated.get_cust_by_mech_purchased_unexposed(cmp, prd_scope_df, prd_scope_nm).drop_duplicates()
    
    # Combined Exposed - Unexposed
    cust_exposed_unexposed = (cust_exposed_by_mech.select("household_id")
                              .unionByName(cust_unexposed_by_mech.select("household_id"))
                              .drop_duplicates()
    )
    
    # Cust mechname exposed, mechname unexposed
     = (cust_exposed_unexposed
         .join(cust_exposed_by_mech.withColumnRenamed("mech_name", "exposed"), "household_id", "left")
         .join(cust_unexposed_by_mech.withColumnRenamed("mech_name", "unexposed"),"household_id", "left")
         .join(cust_purchased_exposed.select("household_id", "mech_name").withColumnRenamed("mech_name", "exposed_purchased"), "left")
         .join(cust_purchased_unexposed.select("household_id", "mech_name").withColumnRenamed("mech_name", "unexposed_purchased"), "left")
        )

    # Movement : prior - pre
    cust_mv = _get_cust_mvmnt_ppp_pre(cmp, prd_scope_df, prd_scope_nm)
    cust_mv.groupBy('customer_mv_group').agg(F.countDistinct('household_id')).display()

    # Flag customer movement and exposure
    movement_and_exposure_by_mech = (cust_exposed_unexposed_purchased
                                     .join(cust_mv.select("household_id", "customer_mv_group").drop_duplicates(), 'household_id', 'inner')
                                     )
    
    # Uplift Calculation by mechanic
    n_cust_total_non_exposed_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Purchased') \
                                                                      .groupBy('customer_mv_group') \
                                                                      .agg(F.countDistinct(F.col('household_id')).alias('Non_exposed_Purchased_all')) \
                                                                      .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Purchased') \
                                                                                   .agg(F.countDistinct(F.col('household_id')) \
                                                                                         .alias('Non_exposed_Purchased_all')).fillna(0) \
                                                                                   .withColumn('customer_mv_group', F.lit('Total')))

    n_cust_total_non_exposed_non_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Non_purchased') \
                                                                          .groupBy('customer_mv_group') \
                                                                          .agg(F.countDistinct(F.col('household_id')).alias('Non_exposed_Non_purchased_all')) \
                                                                          .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Non_exposed_Non_purchased') \
                                                                                       .agg(F.countDistinct(F.col('household_id')) \
                                                                                             .alias('Non_exposed_Non_purchased_all')).fillna(0) \
                                                                                       .withColumn('customer_mv_group', F.lit('Total')))


    n_cust_total_exposed_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Purchased') \
                                                                  .groupBy('customer_mv_group') \
                                                                  .agg(F.countDistinct(F.col('household_id')).alias('Exposed_Purchased_all')) \
                                                                  .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Purchased') \
                                                                               .agg(F.countDistinct(F.col('household_id')) \
                                                                                     .alias('Exposed_Purchased_all')).fillna(0) \
                                                                               .withColumn('customer_mv_group', F.lit('Total')))



    n_cust_total_exposed_non_purchased = movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Non_purchased') \
                                                                      .groupBy('customer_mv_group') \
                                                                      .agg(F.countDistinct(F.col('household_id')).alias('Exposed_Non_purchased_all'))\
                                                                      .unionByName(movement_and_exposure_by_mech.filter(F.col('group') == 'Exposed_Non_purchased') \
                                                                                   .agg(F.countDistinct(F.col('household_id')) \
                                                                                         .alias('Exposed_Non_purchased_all')).fillna(0) \
                                                                                   .withColumn('customer_mv_group', F.lit('Total')))



    # Total customers for Exposed Purchased and Exposed Non-purchased per each mechanic (if more than 1 mechanic)
    if num_of_mechanics > 1:

        n_cust_total_exposed_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_exposed_purchased,
                                                                ex_pur_group='Exposed_Purchased',
                                                                movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                mechanic_list=mechanic_list)

        n_cust_total_exposed_non_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_exposed_non_purchased,
                                                                    ex_pur_group='Exposed_Non_purchased',
                                                                    movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                    mechanic_list=mechanic_list)

        n_cust_total_non_exposed_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_non_exposed_purchased,
                                                                    ex_pur_group='Non_exposed_Purchased',
                                                                    movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                    mechanic_list=mechanic_list)

        n_cust_total_non_exposed_non_purchased = _get_total_cust_per_mech(n_cust_total=n_cust_total_non_exposed_non_purchased,
                                                                        ex_pur_group='Non_exposed_Non_purchased',
                                                                        movement_and_exposure_by_mech=movement_and_exposure_by_mech,
                                                                        mechanic_list=mechanic_list)


    combine_n_cust = n_cust_total_non_exposed_purchased.join(n_cust_total_non_exposed_non_purchased, on='customer_mv_group', how='left') \
                                                       .join(n_cust_total_exposed_purchased, on='customer_mv_group', how='left') \
                                                       .join(n_cust_total_exposed_non_purchased, on='customer_mv_group', how='left')

#     combine_n_cust.show()


    ## Conversion and Uplift New Logic
    # Get basic calcuations of conversion rates, uplift percent and number of customers
    results = combine_n_cust.withColumn('uplift_lv', F.lit(cust_uplift_lv)) \
                            .withColumn('mechanic', F.lit('all')) \
                            .withColumn('num_exposed_buy', F.col('Exposed_Purchased_all')) \
                            .withColumn('num_exposed_not_buy', F.col('Exposed_Non_purchased_all')) \
                            .withColumn('num_unexposed_buy', F.col('Non_exposed_Purchased_all')) \
                            .withColumn('num_unexposed_not_buy', F.col('Non_exposed_Non_purchased_all')) \
                            .withColumn('cvs_rate_exposed',
                                        F.col('Exposed_Purchased_all') / (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all'))) \
                            .withColumn('cvs_rate_unexposed',
                                        F.col('Non_exposed_Purchased_all') / (F.col('Non_exposed_Purchased_all') + F.col('Non_exposed_Non_purchased_all'))) \
                            .withColumn('pct_uplift',
                                        (F.col('cvs_rate_exposed') / (F.col('cvs_rate_unexposed'))) - 1) \
                            .withColumn('uplift_cust',
                                        (F.col('cvs_rate_exposed') - F.col('cvs_rate_unexposed')) *
                                        (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all')))

    # Get only positive customer uplift for each customer group (New/Lapse/Existing)
    pstv_cstmr_uplift_all_mech_col = results.select('customer_mv_group', 'uplift_cust').filter("customer_mv_group <> 'Total'") \
                                            .withColumn('pstv_cstmr_uplift',
                                                        F.when(F.col('uplift_cust') > 0, F.col('uplift_cust')).otherwise(0))

    # Get Total customer uplift, ignoring negative values
    pstv_cstmr_uplift_all_mech_col = pstv_cstmr_uplift_all_mech_col.select('customer_mv_group', 'pstv_cstmr_uplift') \
                                                                   .unionByName(pstv_cstmr_uplift_all_mech_col.agg(F.sum(F.col('pstv_cstmr_uplift')) \
                                                                                                                    .alias('pstv_cstmr_uplift')).fillna(0) \
                                                                                                              .withColumn('customer_mv_group', F.lit('Total')))

    results = results.join(pstv_cstmr_uplift_all_mech_col.select('customer_mv_group', 'pstv_cstmr_uplift'), on='customer_mv_group', how='left')

    # Recalculate uplift using total positive customers
    results = results.withColumn('pct_positive_cust_uplift',
                                 (F.col('pstv_cstmr_uplift') / (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all'))) / F.col('cvs_rate_unexposed'))
    
    # Sort row order , export as SparkFrame
    df = results.select('customer_mv_group',
                        'mechanic',
                        'num_exposed_buy',
                        'num_exposed_not_buy',
                        'num_unexposed_buy',
                        'num_unexposed_not_buy',
                        'cvs_rate_exposed',
                        'cvs_rate_unexposed',
                        'pct_uplift',
                        'uplift_cust',
                        'pstv_cstmr_uplift',
                        'pct_positive_cust_uplift').toPandas()
    sort_dict = {"new":0, "existing":1, "lapse":2, "Total":3}
    df = df.sort_values(by=["customer_mv_group"], key=lambda x: x.map(sort_dict))  # type: ignore
    results = spark.createDataFrame(df)

    # Repeat for all mechanics if multiple mechanics
    if num_of_mechanics > 1:
        mech_result = {}
        pstv_cstmr_uplift_col = {}

        for mech in mechanic_list:
            mech_result[mech] = combine_n_cust.withColumn('mechanic', F.lit(mech)) \
                                              .withColumn('num_exposed_buy', F.col('Exposed_Purchased_' + mech)) \
                                              .withColumn('num_exposed_not_buy', F.col('Exposed_Non_purchased_' + mech)) \
                                              .withColumn('num_unexposed_buy', F.col('Non_exposed_Purchased_' + mech)) \
                                              .withColumn('num_unexposed_not_buy', F.col('Non_exposed_Non_purchased_' + mech)) \
                                              .withColumn('cvs_rate_exposed',
                                                          F.col('Exposed_Purchased_' + mech) /
                                                          (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech))) \
                                              .withColumn('cvs_rate_unexposed',
                                                          F.col('Non_exposed_Purchased_' + mech) / (F.col('Non_exposed_Purchased_' + mech) + F.col('Non_exposed_Non_purchased_' + mech))) \
                                              .withColumn('pct_uplift',
                                                          (F.col('cvs_rate_exposed') / (F.col('cvs_rate_unexposed'))) - 1) \
                                              .withColumn('uplift_cust',
                                                          (F.col('cvs_rate_exposed') - F.col('cvs_rate_unexposed')) *
                                                          (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech)))

            pstv_cstmr_uplift_col[mech] = mech_result[mech].select('customer_mv_group', 'uplift_cust').filter("customer_mv_group <> 'Total'") \
                                                           .withColumn('pstv_cstmr_uplift',
                                                                       F.when(F.col('uplift_cust') > 0, F.col('uplift_cust')).otherwise(0))

            pstv_cstmr_uplift_col[mech] = pstv_cstmr_uplift_col[mech].select('customer_mv_group', 'pstv_cstmr_uplift') \
                                                                     .unionByName(pstv_cstmr_uplift_col[mech].agg(F.sum(F.col('pstv_cstmr_uplift')) \
                                                                                                                   .alias('pstv_cstmr_uplift')).fillna(0) \
                                                                                                             .withColumn('customer_mv_group', F.lit('Total')))

            mech_result[mech] = mech_result[mech].join(pstv_cstmr_uplift_col[mech].select('customer_mv_group', 'pstv_cstmr_uplift'), on='customer_mv_group', how='left')

            mech_result[mech] = mech_result[mech].withColumn('pct_positive_cust_uplift',
                                                             (F.col('pstv_cstmr_uplift') /
                                                              (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech))) /
                                                             F.col('cvs_rate_unexposed'))
            
            # Sort row order , export as SparkFrame
            df = mech_result[mech].toPandas()
            sort_dict = {"new":0, "existing":1, "lapse":2, "Total":3}
            df = df.sort_values(by=["customer_mv_group"], key=lambda x: x.map(sort_dict))  # type: ignore
            mech_result[mech] = spark.createDataFrame(df)

            results = results.unionByName(mech_result[mech].select('customer_mv_group',
                                                                   'mechanic',
                                                                   'num_exposed_buy',
                                                                   'num_exposed_not_buy',
                                                                   'num_unexposed_buy',
                                                                   'num_unexposed_not_buy',
                                                                   'cvs_rate_exposed',
                                                                   'cvs_rate_unexposed',
                                                                   'pct_uplift',
                                                                   'uplift_cust',
                                                                   'pstv_cstmr_uplift',
                                                                   'pct_positive_cust_uplift'))

    return results, movement_and_exposure_by_mech