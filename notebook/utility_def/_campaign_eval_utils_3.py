# Databricks notebook source
# MAGIC %md
# MAGIC # Import all library

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

from pyspark import StorageLevel

## import longging and traceback
import logging
import traceback
import errno

## datetime lib
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
import time

## pandas and numpy
import pandas as pd
import numpy as np
import math as math

## os path
import os
import sys
import string
import subprocess
import importlib
import shutil
import urllib 
import pathlib

# COMMAND ----------

# MAGIC %run /Workspace/Repos/niti.buesamae@lotuss.com/edm_media_test/notebook/utility_def/edm_utils

# COMMAND ----------

def get_customer_uplift_per_mechanic(txn: SparkDataFrame,
                                     cp_start_date: str,
                                     cp_end_date: str,
                                     wk_type: str,
                                     test_store_sf: SparkDataFrame,
                                     adj_prod_sf: SparkDataFrame,
                                     brand_sf: SparkDataFrame,
                                     feat_sf: SparkDataFrame,
                                     ctr_store_list: List,
                                     cust_uplift_lv: str,
                                     store_matching_df_var: SparkDataFrame):
    """Customer Uplift : Exposed vs Unexposed
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
    # spark.sql('DROP TABLE IF EXISTS tdm_seg.cust_uplift_by_mech_temp' + username_str)
    # movement_and_exposure_by_mech.write.saveAsTable('tdm_seg.cust_uplift_by_mech_temp' + username_str)
    # movement_and_exposure_by_mech = spark.table('tdm_seg.cust_uplift_by_mech_temp' + username_str)

    spark.sql('DROP TABLE IF EXISTS tdm_dev.cust_uplift_by_mech_temp' + username_str)
    movement_and_exposure_by_mech.write.saveAsTable('tdm_dev.cust_uplift_by_mech_temp' + username_str)
    movement_and_exposure_by_mech = spark.table('tdm_dev.cust_uplift_by_mech_temp' + username_str)

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

# COMMAND ----------

def get_cust_activated_by_mech(txn: SparkDataFrame,
                               cp_start_date: str,
                               cp_end_date: str,
                               wk_type: str,
                               test_store_sf: SparkDataFrame,
                               adj_prod_sf: SparkDataFrame,
                               brand_sf: SparkDataFrame,
                               feat_sf: SparkDataFrame,
                               promozone_flag : bool = False):
    """Get customer exposed & unexposed / shopped, not shop

    Parameters
    ----------
    txn:
        Snapped transaction of ppp + pre + cmp period
    cp_start_date
    cp_end_date
    wk_type:
        "fis_week" or "promo_week"
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')

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
            )
        return filled_test_store_sf

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
             .filter(F.col('offline_online_other_channel') == 'OFFLINE')
             .join(prd_scope_df, 'upc_id')
             .select('household_id', 'transaction_uid', 'tran_datetime', 'store_id', 'date_id')
             .drop_duplicates()
            )
        return out

    # Get the "last seen" mechanic(s) that a shopper saw before they make purchases
    def _get_activ_mech_last_seen(txn: SparkDataFrame, 
                                  test_store_sf: SparkDataFrame,
                                  adj_prod_sf: SparkDataFrame,
                                  period_wk_col: str,
                                  prd_scope_df: SparkDataFrame, 
                                  cp_start_date: str,
                                  cp_end_date: str,
                                  promozone_flag: bool
                                 ) -> SparkDataFrame:

        # Get all featured shopping transactions during campaign
        all_feat_trans_item_level = _get_all_feat_trans(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=prd_scope_df)

        all_feat_trans_trans_level = all_feat_trans_item_level.select('household_id', 'transaction_uid', 'tran_datetime', 'store_id', 'date_id') \
                                                              .filter(F.col('date_id').between(cp_start_date, cp_end_date)) \
                                                              .drop_duplicates()
        
        # For Promozone, only take into account transactions occuring only at Target stores, since the zones are only available at Target stores
        if promozone_flag == True:
            test_store_list = test_store_sf.select('store_id').drop_duplicates().rdd.flatMap(lambda x: x).collect()
            
            all_feat_trans_trans_level = all_feat_trans_trans_level.filter(F.col('store_id').isin(test_store_list))

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

        return purchased_exposure_flagged

    #---- Main
    print("-"*80)
    print("Customer Media Exposed -> Activated")
    print("Media Exposed = shopped in media aisle within campaign period (base on target input file) at target store , channel OFFLINE ")
    print("Activate = Exposed & Shop (Feature SKU/Feature Brand) in campaign period at any store format and any channel")
    print("-"*80)
    if adj_prod_sf is None:
        print("Media exposed use total store level (all products)")
        adj_prod_sf = _create_adj_prod_df(txn)
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    # Get target stores with filled dates and replace special characters in mechanic names 
    target_str = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)

    mechanic_list = target_str.select('mech_name').drop_duplicates().rdd.flatMap(lambda x: x).collect()
    print("List of detected mechanics from store list: ", mechanic_list)

    num_of_mechanics = len(mechanic_list)
    
    # Get activated customers at Brand level using "last seen" method
    cmp_shppr_last_seen_brand  = _get_activ_mech_last_seen(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf, 
                                                           period_wk_col=period_wk_col, prd_scope_df=brand_sf,
                                                           cp_start_date=cp_start_date, cp_end_date=cp_end_date,
                                                           promozone_flag=promozone_flag)
    
    # Add view to test
    cmp_shppr_last_seen_brand.show()

    cmp_shppr_last_seen_brand_tag = cmp_shppr_last_seen_brand.withColumn('level', F.lit('brand')) \
                                                             .withColumn('total_mechanics_exposed',
                                                                         np.sum(cmp_shppr_last_seen_brand[col] for col in cmp_shppr_last_seen_brand.columns[1:num_of_mechanics+1]))
    
    print('Activated customers at brand level by number of mechanics exposed:')
    cmp_shppr_last_seen_brand_tag.groupBy('level').pivot('total_mechanics_exposed').agg(F.countDistinct(F.col('household_id'))).show()
    
    # Get activated customers at SKU level using "last seen" method
    cmp_shppr_last_seen_sku  = _get_activ_mech_last_seen(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf, 
                                                         period_wk_col=period_wk_col, prd_scope_df=feat_sf,
                                                         cp_start_date=cp_start_date, cp_end_date=cp_end_date,
                                                         promozone_flag=promozone_flag)
    
    cmp_shppr_last_seen_sku_tag = cmp_shppr_last_seen_sku.withColumn('level', F.lit('sku')) \
                                                         .withColumn('total_mechanics_exposed',
                                                                     np.sum(cmp_shppr_last_seen_sku[col] for col in cmp_shppr_last_seen_sku.columns[1:num_of_mechanics+1]))
    
    print('Activated customers at SKU level by number of mechanics exposed:')
    cmp_shppr_last_seen_sku_tag.groupBy('level').pivot('total_mechanics_exposed').agg(F.countDistinct(F.col('household_id'))).show()

    # Get numbers of activated customers for all mechanics for brand and SKU levels
    activated_brand_num = cmp_shppr_last_seen_brand_tag.groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                       .withColumn('mechanic', F.lit('all'))
    
    activated_sku_num = cmp_shppr_last_seen_sku_tag.groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                   .withColumn('mechanic', F.lit('all'))
    
    # Add by mech if there are more than 1 mechanic
    if num_of_mechanics > 1:
        mech_result_brand = {}
        mech_result_sku = {}
        
        for mech in mechanic_list:
            mech_result_brand[mech] = cmp_shppr_last_seen_brand_tag.filter(F.col(mech) == 1) \
                                                                   .groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                                   .withColumn('mechanic', F.lit(mech))
            
            mech_result_sku[mech] = cmp_shppr_last_seen_sku_tag.filter(F.col(mech) == 1) \
                                                               .groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                               .withColumn('mechanic', F.lit(mech))

            activated_brand_num = activated_brand_num.unionByName(mech_result_brand[mech])
            activated_sku_num = activated_sku_num.unionByName(mech_result_sku[mech])
            
    activated_both_num = activated_brand_num.unionByName(activated_sku_num).select('level', 'mechanic', 'num_activated')
    
    print('Number of customers activated by each mechanic: ')
    activated_both_num.show()

    return cmp_shppr_last_seen_brand_tag, cmp_shppr_last_seen_sku_tag, activated_both_num

# COMMAND ----------

def get_sales_mkt_growth_per_mech(
    txn, week_type, store_format, trg_store_df, feat_list, brand_df, cate_df, cate_level
):
    """
    Calculates the percentage of sales market growth per mechanism using the given parameters.

    :param txn: DataFrame containing transaction data.
    :param week_type: Week type of transactions to be considered.
    :param store_format: Store format to be considered.
    :param trg_store_df: DataFrame containing target store data.
    :param feat_list: List of features to be considered in the model.
    :param brand_df: DataFrame containing brand data.
    :return: DataFrame with percentage of sales market growth per mechanism.
    """
    """Get customer exposed & unexposed / shopped, not shop

    Parameters
    ----------
    txn:
        Snapped transaction of ppp + pre + cmp period
    cp_start_date
    cp_end_date
    wk_type:
        "fis_week" or "promo_week"
    """
    # get mechanic list
    mechanic_list = (
        trg_store_df.select("mech_name")
        .drop_duplicates()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    print("List of detected mechanics from store list: ", mechanic_list)

    num_of_mechanics = len(mechanic_list)

    if store_format == "":
        print("Not define store format, will use HDE as default")
        store_format = "HDE"
    # end if

    # Select week period column name
    if week_type == "fis_wk":
        period_col = "period_fis_wk"
        week_col = "week_id"
    elif week_type == "promo_wk":
        period_col = "period_promo_wk"
        week_col = "promoweek_id"
    else:
        print("In correct week_type, Will use fis_week as default value \n")
        period_col = "period_fis_wk"
        week_col = "week_id"
    # filter txn for category (class) level and brand level
    txn_pre_cond = """ ({0} == 'pre') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(
        period_col, store_format
    )

    txn_dur_cond = """ ({0} == 'cmp') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(
        period_col, store_format
    )

    def _split_mech(txn_mech_pre_trg):
        purchased_exposure_count = (
            txn_mech_pre_trg.groupBy("household_id")
            .pivot("mech_name")
            .agg(F.countDistinct(F.col("transaction_uid")))
            .fillna(0)
        )
        mech_name_columns = purchased_exposure_count.columns
        mech_name_columns.remove("household_id")
        purchased_exposure_flagged = purchased_exposure_count.select("*")
        for col in mech_name_columns:
            purchased_exposure_flagged = purchased_exposure_flagged.withColumn(
                col, F.when(F.col(col) == 0, 0).otherwise(1)
            )
        return purchased_exposure_flagged

    txn_cate = txn.join(cate_df, "upc_id", "left_semi").join(
        trg_store_df, [txn.store_id == trg_store_df.store_id], "inner"
    )
    txn_brand = txn.join(brand_df, "upc_id", "left_semi").join(
        trg_store_df, [txn.store_id == trg_store_df.store_id], "inner"
    )
    txn_sku = txn.filter(F.col("upc_id").isin(feat_list)).join(
        trg_store_df, [txn.store_id == trg_store_df.store_id], "inner"
    )
    txn_columns = txn.columns
    mech_cate = _split_mech(txn_cate)
    mech_brand = _split_mech(txn_brand)
    mech_sku = _split_mech(txn_sku)
    # join with mech
    # add cate
    txn_mech_pre_trg_cate = (
        txn_cate.join(mech_cate, "household_id", "left")
        .where(txn_pre_cond)
    )
    txn_mech_dur_trg_cate = (
        txn_cate.join(mech_cate, "household_id", "left")
        .where(txn_dur_cond)
    )
    # add brand
    txn_mech_pre_trg_brand = (
        txn_brand.join(mech_brand, "household_id", "left")
        .where(txn_pre_cond)
    )
    txn_mech_dur_trg_brand = (
        txn_brand.join(mech_brand, "household_id", "left")
        .where(txn_dur_cond)
    )
    # add sku
    txn_mech_pre_trg_sku = (
        txn_sku.join(mech_sku, "household_id", "left")
        .where(txn_pre_cond)
    )

    txn_mech_dur_trg_sku = (
        txn_sku.join(mech_sku, "household_id", "left")
        .where(txn_dur_cond)
    )
    del [txn_cate, txn_brand, txn_sku]

    def _get_growth(
        txn_mech_pre_trg_cate,
        txn_mech_dur_trg_cate,
        txn_mech_pre_trg_brand,
        txn_mech_dur_trg_brand,
        txn_mech_pre_trg_sku,
        txn_mech_dur_trg_sku,
    ):
        # for fis_week
        if week_col == "week_id":
            # target cate
            cate_pre_trg_sum = txn_mech_pre_trg_cate.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            cate_dur_trg_sum = txn_mech_dur_trg_cate.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
            # target brand
            brand_pre_trg_sum = txn_mech_pre_trg_brand.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            brand_dur_trg_sum = txn_mech_dur_trg_brand.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
            # target sku
            sku_pre_trg_sum = txn_mech_pre_trg_sku.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            sku_dur_trg_sum = txn_mech_dur_trg_sku.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
        else:  # use promo week
            # target cate
            cate_pre_trg_sum = txn_mech_pre_trg_cate.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            cate_dur_trg_sum = txn_mech_dur_trg_cate.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
            # target brand
            brand_pre_trg_sum = txn_mech_pre_trg_brand.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            brand_dur_trg_sum = txn_mech_dur_trg_brand.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
            # target sku
            sku_pre_trg_sum = txn_mech_pre_trg_sku.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            sku_dur_trg_sum = txn_mech_dur_trg_sku.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
        # end if

        # target
        # average in each period
        cate_pre_trg_agg = cate_pre_trg_sum.agg(
            F.avg("sales").alias("cate_pre_trg_avg"),
            F.sum("sales").alias("cate_pre_trg_ssum"),
        )
        cate_dur_trg_agg = cate_dur_trg_sum.agg(
            F.avg("sales").alias("cate_dur_trg_avg"),
            F.sum("sales").alias("cate_dur_trg_ssum"),
        )
        brand_pre_trg_agg = brand_pre_trg_sum.agg(
            F.avg("sales").alias("brand_pre_trg_avg"),
            F.sum("sales").alias("brand_pre_trg_ssum"),
        )
        brand_dur_trg_agg = brand_dur_trg_sum.agg(
            F.avg("sales").alias("brand_dur_trg_avg"),
            F.sum("sales").alias("brand_dur_trg_ssum"),
        )
        sku_pre_trg_agg = sku_pre_trg_sum.agg(
            F.avg("sales").alias("sku_pre_trg_avg"),
            F.sum("sales").alias("sku_pre_trg_ssum"),
        )
        sku_dur_trg_agg = sku_dur_trg_sum.agg(
            F.avg("sales").alias("sku_dur_trg_avg"),
            F.sum("sales").alias("sku_dur_trg_ssum"),
        )

        cate_pre_trg_ssum, cate_pre_trg_avg = cate_pre_trg_agg.select(
            "cate_pre_trg_ssum", "cate_pre_trg_avg"
        ).collect()[0]
        cate_dur_trg_ssum, cate_dur_trg_avg = cate_dur_trg_agg.select(
            "cate_dur_trg_ssum", "cate_dur_trg_avg"
        ).collect()[0]

        brand_pre_trg_ssum, brand_pre_trg_avg = brand_pre_trg_agg.select(
            "brand_pre_trg_ssum", "brand_pre_trg_avg"
        ).collect()[0]
        brand_dur_trg_ssum, brand_dur_trg_avg = brand_dur_trg_agg.select(
            "brand_dur_trg_ssum", "brand_dur_trg_avg"
        ).collect()[0]

        sku_pre_trg_ssum, sku_pre_trg_avg = sku_pre_trg_agg.select(
            "sku_pre_trg_ssum", "sku_pre_trg_avg"
        ).collect()[0]
        sku_dur_trg_ssum, sku_dur_trg_avg = sku_dur_trg_agg.select(
            "sku_dur_trg_ssum", "sku_dur_trg_avg"
        ).collect()[0]

        del [
            cate_pre_trg_agg,
            cate_dur_trg_agg,
            brand_pre_trg_agg,
            brand_dur_trg_agg,
            sku_pre_trg_agg,
            sku_dur_trg_agg,
        ]
        # --------------------------------------
        # get sales growth pre vs during -> use average weekly Product level test vs control
        # --------------------------------------
        ## cate growth
        if cate_pre_trg_avg is None:
            print(' "cate_pre_trg_avg" has no value, will set value to = 0 \n ')
            cate_pre_trg_avg = 0
        # end if
        cate_wkly_growth = div_zero_chk(
            (cate_dur_trg_avg - cate_pre_trg_avg), cate_pre_trg_avg
        )

        if sku_pre_trg_avg is None:
            print(' "sku_pre_trg_avg" has no value, will set value to = 0 \n ')
            sku_pre_trg_avg = 0
        # end if
        trg_wkly_growth = div_zero_chk(
            (sku_dur_trg_avg - sku_pre_trg_avg), sku_pre_trg_avg
        )

        if brand_pre_trg_avg is None:
            print(' "brand_pre_trg_avg" has no value, will set value to = 0 \n ')
            brand_pre_trg_avg = 0
        # end if

        if brand_pre_trg_ssum is None:
            print(' "brand_pre_trg_ssum" has no value, will set value to = 0 \n ')
            brand_pre_trg_ssum = 0
        # end if

        brand_wkly_growth = div_zero_chk(
            (brand_dur_trg_avg - brand_pre_trg_avg), brand_pre_trg_avg
        )

        trg_mkt_pre = div_zero_chk(
            sku_pre_trg_ssum, cate_pre_trg_ssum
        )  # sku mkt share pre
        trg_mkr_dur = div_zero_chk(
            sku_dur_trg_ssum, cate_dur_trg_ssum
        )  # sku mkt share pre
        trg_mkt_growth = div_zero_chk((trg_mkr_dur - trg_mkt_pre), trg_mkt_pre)

        # dummy = 0 / brand_pre_trg_ssum if brand_pre_trg_ssum is not None else 0
        dummy = div_zero_chk(
            0, brand_pre_trg_ssum
        )
        pd_cols = ["level", "str_fmt", "pre", "during", "week_type"]
        pd_data = [
            [
                f"{cate_level}_wk_avg",
                "Test",
                cate_pre_trg_avg,
                cate_dur_trg_avg,
                week_col,
            ],
            ["brand_wk_avg", "Test", brand_pre_trg_avg, brand_dur_trg_avg, week_col],
            ["sku_wk_avg", "Test", sku_pre_trg_avg, sku_dur_trg_avg, week_col],
            [f"{cate_level}_wk_grw", "Test", dummy, cate_wkly_growth, week_col],
            ["brand_wk_grw", "Test", dummy, brand_wkly_growth, week_col],
            ["sku_wk_grw", "Test", dummy, trg_wkly_growth, week_col],
            ["MKT_SHARE", "Test", trg_mkt_pre, trg_mkr_dur, week_col],
            [
                f"{cate_level}_sales",
                "Test",
                cate_pre_trg_ssum,
                cate_dur_trg_ssum,
                week_col,
            ],
            ["brand_sales", "Test", brand_pre_trg_ssum, brand_dur_trg_ssum, week_col],
            ["sku_sales", "Test", sku_pre_trg_ssum, sku_dur_trg_ssum, week_col],
            ["mkt_growth", "Test", dummy, trg_mkt_growth, week_col],
        ]

        sales_info_pd = pd.DataFrame(pd_data, columns=pd_cols)
        for name in [
            "pre",
            "during",
        ]:
            sales_info_pd[name] = sales_info_pd[name].astype(float)
        return spark.createDataFrame(sales_info_pd)

    mech_growth = _get_growth(
        txn_mech_pre_trg_cate,
        txn_mech_dur_trg_cate,
        txn_mech_pre_trg_brand,
        txn_mech_dur_trg_brand,
        txn_mech_pre_trg_sku,
        txn_mech_dur_trg_sku,
    ).withColumn("mechanic", F.lit("all"))
    # Add by mech if there are more than 1 mechanic
    if num_of_mechanics > 1:
        mech_results = {}

        for mech in mechanic_list:
            mech_results[mech] = _get_growth(
                txn_mech_pre_trg_cate.filter(F.col(mech) == 1),
                txn_mech_dur_trg_cate.filter(F.col(mech) == 1),
                txn_mech_pre_trg_brand.filter(F.col(mech) == 1),
                txn_mech_dur_trg_brand.filter(F.col(mech) == 1),
                txn_mech_pre_trg_sku.filter(F.col(mech) == 1),
                txn_mech_dur_trg_sku.filter(F.col(mech) == 1),
            ).withColumn("mechanic", F.lit(mech))
            mech_growth = mech_growth.unionByName(mech_results[mech], allowMissingColumns=True)
    return mech_growth

# COMMAND ----------

### Scent Dev 2024 By Customer by mech 
def cust_kpi_noctrl_pm_dev(txn
                    ,store_fmt
                    ,test_store_sf
                    ,feat_list
                    ,brand_df
                    ,cate_df
                    ):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
    - Category dataframe (all SKU in category at defined switching level)
    - Week period
    - Return 
      >> combined_kpi : spark dataframe with all combine KPI
      >> kpi_df : combined_kpi in pandas
      >> df_pv : Pivot format of kpi_df
    """ 

    features_brand    = brand_df
    features_category = cate_df
    

    #test_store_sf:pyspark.sql.dataframe.DataFrame
    #store_id:integer
    #c_start:date
    #c_end:date
    #mech_count:integer
    #mech_name:string
    #media_fee_psto:double
    #aisle_subclass:string
    #aisle_scope:string
    
    kpis = [(F.countDistinct('date_id')/7).alias('n_weeks'),
            F.sum('net_spend_amt').alias('sales'),
            F.sum('pkg_weight_unit').alias('units'),
            F.countDistinct('transaction_uid').alias('visits'),
            F.countDistinct('household_id').alias('customer'),
           (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
           (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
           (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
           (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
           (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]
        
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().groupBy("store_id").agg(F.concat_ws("_", F.collect_list("mech_name")).alias("mech_name")).dropDuplicates().persist()
      
      txn_test_pre = txn_all.filter(txn_all.period_promo_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_promo_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
    else:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().persist()
      txn_test_pre = txn_all.filter(txn_all.period_promo_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_promo_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                        .dropDuplicates()

    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp, allowMissingColumns=True)
     .withColumn('carded_nonCarded', F.when(F.col('household_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    sku_kpi = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                              .groupBy('period_promo_wk',"mech_name")\
                              .pivot('carded_nonCarded')\
                              .agg(*kpis)\
                              .withColumn('kpi_level', F.lit('feature_sku'))
                              
    brand_in_cate_kpi = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                                .groupBy('period_promo_wk',"mech_name")\
                                                .pivot('carded_nonCarded')\
                                                .agg(*kpis)\
                                                .withColumn('kpi_level', F.lit('feature_brand'))
        
        
    all_category_kpi = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                       .groupBy('period_promo_wk',"mech_name")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_category'))
    # scent add 2024-04-02                              
    all_lotus_kpi = txn_test_combine.groupBy('period_promo_wk',"mech_name")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_lotus'))                           
    
    combined_kpi_final = sku_kpi.unionByName(brand_in_cate_kpi, allowMissingColumns=True)\
                                .unionByName(all_category_kpi, allowMissingColumns=True)\
                                .unionByName(all_lotus_kpi, allowMissingColumns=True)\
                                .fillna(0)

    kpi_df = to_pandas(combined_kpi_final) 
    
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      sku_kpi_all = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                                .groupBy('period_promo_wk')\
                                .pivot('carded_nonCarded')\
                                .agg(*kpis)\
                                .withColumn('kpi_level', F.lit('feature_sku'))\
                                .withColumn('mech_name', F.lit('ALL_Mech'))
                                
      brand_in_cate_kpi_all = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                              .groupBy('period_promo_wk')\
                                              .pivot('carded_nonCarded')\
                                              .agg(*kpis)\
                                              .withColumn('kpi_level', F.lit('feature_brand'))\
                                              .withColumn('mech_name', F.lit('ALL_Mech'))
      
      
      all_category_kpi_all = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                        .groupBy('period_promo_wk')\
                                        .pivot('carded_nonCarded')\
                                        .agg(*kpis)\
                                        .withColumn('kpi_level', F.lit('feature_category'))\
                                        .withColumn('mech_name', F.lit('ALL_Mech'))
      #Scent add 2024-04-02
      all_lotus_kpi_all = txn_test_combine.groupBy('period_promo_wk')\
                                        .pivot('carded_nonCarded')\
                                        .agg(*kpis)\
                                        .withColumn('kpi_level', F.lit('feature_lotus'))\
                                        .withColumn('mech_name', F.lit('ALL_Mech'))

      combined_kpi_all = sku_kpi_all.unionByName(brand_in_cate_kpi_all, allowMissingColumns=True)\
                                    .unionByName(all_category_kpi_all, allowMissingColumns=True)\
                                    .unionByName(all_lotus_kpi_all, allowMissingColumns=True)\
                                    .fillna(0)

      kpi_df = to_pandas(combined_kpi_final.unionByName(combined_kpi_all, allowMissingColumns=True))
    
    df_pv = kpi_df[['period_promo_wk', 'kpi_level', 'MyLo_customer','mech_name']].pivot(index='period_promo_wk', columns=['kpi_level','mech_name'], values='MyLo_customer')
    
    #df_pv['%cust_sku_in_category']   = df_pv['feature_sku']/df_pv['feature_category']*100
    #df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    #df_pv['%cust_brand_in_category'] = df_pv['feature_brand']/df_pv['feature_category']*100
    #df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    
    mech_unique = kpi_df["mech_name"].unique()
    for i in mech_unique:
        df_pv[('%cust_sku_in_category', i)] = df_pv[('feature_sku', i)] / df_pv[('feature_category', i)] * 100
        df_pv[('%cust_brand_in_category', i)] = df_pv[('feature_brand', i)] / df_pv[('feature_category', i)] * 100
    
    df_pv.sort_index(ascending=False, inplace=True)
    
    cust_share_pd = df_pv.T.reset_index()
    
    return combined_kpi_final, kpi_df, cust_share_pd

# COMMAND ----------

### Scent Dev 2024 By Customer by mech 
def cust_kpi_noctrl_fiswk_dev(txn
                    ,store_fmt
                    ,test_store_sf
                    ,feat_list
                    ,brand_df
                    ,cate_df
                    ):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
    - Category dataframe (all SKU in category at defined switching level)
    - Week period
    - Return 
      >> combined_kpi : spark dataframe with all combine KPI
      >> kpi_df : combined_kpi in pandas
      >> df_pv : Pivot format of kpi_df
    """ 

    features_brand    = brand_df
    features_category = cate_df
    

    #test_store_sf:pyspark.sql.dataframe.DataFrame
    #store_id:integer
    #c_start:date
    #c_end:date
    #mech_count:integer
    #mech_name:string
    #media_fee_psto:double
    #aisle_subclass:string
    #aisle_scope:string
    
    kpis = [(F.countDistinct('date_id')/7).alias('n_weeks'),
            F.sum('net_spend_amt').alias('sales'),
            F.sum('pkg_weight_unit').alias('units'),
            F.countDistinct('transaction_uid').alias('visits'),
            F.countDistinct('household_id').alias('customer'),
           (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
           (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
           (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
           (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
           (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]
        
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().groupBy("store_id").agg(F.concat_ws("_", F.collect_list("mech_name")).alias("mech_name")).dropDuplicates().persist()
      
      txn_test_pre = txn_all.filter(txn_all.period_fis_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_fis_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
    else:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().persist()
      txn_test_pre = txn_all.filter(txn_all.period_fis_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_fis_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                            .dropDuplicates()

    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp, allowMissingColumns=True)
     .withColumn('carded_nonCarded', F.when(F.col('household_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    sku_kpi = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                              .groupBy('period_fis_wk',"mech_name")\
                              .pivot('carded_nonCarded')\
                              .agg(*kpis)\
                              .withColumn('kpi_level', F.lit('feature_sku'))
                              
    brand_in_cate_kpi = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                                .groupBy('period_fis_wk',"mech_name")\
                                                .pivot('carded_nonCarded')\
                                                .agg(*kpis)\
                                                .withColumn('kpi_level', F.lit('feature_brand'))
        
        
    all_category_kpi = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                       .groupBy('period_fis_wk',"mech_name")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_category'))
    
    combined_kpi_final = sku_kpi.unionByName(brand_in_cate_kpi, allowMissingColumns = True)\
                                .unionByName(all_category_kpi, allowMissingColumns = True)\
                                .fillna(0)
    kpi_df = to_pandas(combined_kpi_final) 
    
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      sku_kpi_all = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                                .groupBy('period_fis_wk')\
                                .pivot('carded_nonCarded')\
                                .agg(*kpis)\
                                .withColumn('kpi_level', F.lit('feature_sku'))\
                                .withColumn('mech_name', F.lit('ALL_Mech'))
                                
      brand_in_cate_kpi_all = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                              .groupBy('period_fis_wk')\
                                              .pivot('carded_nonCarded')\
                                              .agg(*kpis)\
                                              .withColumn('kpi_level', F.lit('feature_brand'))\
                                              .withColumn('mech_name', F.lit('ALL_Mech'))
      
      
      all_category_kpi_all = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                        .groupBy('period_fis_wk')\
                                        .pivot('carded_nonCarded')\
                                        .agg(*kpis)\
                                        .withColumn('kpi_level', F.lit('feature_category'))\
                                        .withColumn('mech_name', F.lit('ALL_Mech'))
      
      combined_kpi_all = sku_kpi_all.unionByName(brand_in_cate_kpi_all, allowMissingColumns = True)\
                                    .unionByName(all_category_kpi_all, allowMissingColumns = True)\
                                    .fillna(0)
      kpi_df = to_pandas(combined_kpi_final.unionByName(combined_kpi_all, allowMissingColumns = True))
    
    df_pv = kpi_df[['period_fis_wk', 'kpi_level', 'MyLo_customer','mech_name']]\
                    .pivot(index='period_fis_wk', columns=['kpi_level','mech_name'], values='MyLo_customer')
    
    #df_pv['%cust_sku_in_category']   = df_pv['feature_sku']/df_pv['feature_category']*100
    #df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    #df_pv['%cust_brand_in_category'] = df_pv['feature_brand']/df_pv['feature_category']*100
    #df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    
    mech_unique = kpi_df["mech_name"].unique()
    for i in mech_unique:
        df_pv[('%cust_sku_in_category', i)] = df_pv[('feature_sku', i)] / df_pv[('feature_category', i)] * 100
        df_pv[('%cust_brand_in_category', i)] = df_pv[('feature_brand', i)] / df_pv[('feature_category', i)] * 100
    
    df_pv.sort_index(ascending=False, inplace=True)
    
    cust_share_pd = df_pv.T.reset_index()
    
    return combined_kpi_final, kpi_df, cust_share_pd

# COMMAND ----------

### Scent Dev 2024 By Customer by mech 
def cust_kpi_noctrl_fiswk_dev_eql(txn
                    ,store_fmt
                    ,test_store_sf
                    ,feat_list
                    ,brand_df
                    ,cate_df
                    ):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
    - Category dataframe (all SKU in category at defined switching level)
    - Week period
    - Return 
      >> combined_kpi : spark dataframe with all combine KPI
      >> kpi_df : combined_kpi in pandas
      >> df_pv : Pivot format of kpi_df
    """ 

    features_brand    = brand_df
    features_category = cate_df
    

    #test_store_sf:pyspark.sql.dataframe.DataFrame
    #store_id:integer
    #c_start:date
    #c_end:date
    #mech_count:integer
    #mech_name:string
    #media_fee_psto:double
    #aisle_subclass:string
    #aisle_scope:string
    
    kpis = [(F.countDistinct('date_id')/7).alias('n_weeks'),
            F.sum('net_spend_amt').alias('sales'),
            F.sum('pkg_weight_unit').alias('units'),
            F.countDistinct('transaction_uid').alias('visits'),
            F.countDistinct('household_id').alias('customer'),
           (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
           (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
           (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
           (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
           (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]
        
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().groupBy("store_id").agg(F.concat_ws("_", F.collect_list("mech_name")).alias("mech_name")).dropDuplicates().persist()
      
      txn_test_pre = txn_all.filter(txn_all.period_eq_fis_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_eq_fis_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
    else:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().persist()
      txn_test_pre = txn_all.filter(txn_all.period_eq_fis_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_eq_fis_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                        .dropDuplicates()

    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp, allowMissingColumns=True)
     .withColumn('carded_nonCarded', F.when(F.col('household_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    sku_kpi = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                              .groupBy('period_eq_fis_wk',"mech_name")\
                              .pivot('carded_nonCarded')\
                              .agg(*kpis)\
                              .withColumn('kpi_level', F.lit('feature_sku'))
                              
    brand_in_cate_kpi = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                                .groupBy('period_eq_fis_wk',"mech_name")\
                                                .pivot('carded_nonCarded')\
                                                .agg(*kpis)\
                                                .withColumn('kpi_level', F.lit('feature_brand'))
        
        
    all_category_kpi = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                       .groupBy('period_eq_fis_wk',"mech_name")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_category'))

    all_lotus_kpi = txn_test_combine.groupBy('period_eq_fis_wk',"mech_name")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_lotus'))                                   
    
    combined_kpi_final = sku_kpi.unionByName(brand_in_cate_kpi, allowMissingColumns=True)\
                                .unionByName(all_category_kpi, allowMissingColumns=True)\
                                  .unionByName(all_lotus_kpi, allowMissingColumns=True)\
                                .fillna(0)

    kpi_df = to_pandas(combined_kpi_final) 
    
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      sku_kpi_all = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                                .groupBy('period_eq_fis_wk')\
                                .pivot('carded_nonCarded')\
                                .agg(*kpis)\
                                .withColumn('kpi_level', F.lit('feature_sku'))\
                                .withColumn('mech_name', F.lit('ALL_Mech'))
                                
      brand_in_cate_kpi_all = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                              .groupBy('period_eq_fis_wk')\
                                              .pivot('carded_nonCarded')\
                                              .agg(*kpis)\
                                              .withColumn('kpi_level', F.lit('feature_brand'))\
                                              .withColumn('mech_name', F.lit('ALL_Mech'))
      
      
      all_category_kpi_all = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                        .groupBy('period_eq_fis_wk')\
                                        .pivot('carded_nonCarded')\
                                        .agg(*kpis)\
                                        .withColumn('kpi_level', F.lit('feature_category'))\
                                        .withColumn('mech_name', F.lit('ALL_Mech'))
      
      all_lotus_kpi_all = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                        .groupBy('period_eq_fis_wk')\
                                        .pivot('carded_nonCarded')\
                                        .agg(*kpis)\
                                        .withColumn('kpi_level', F.lit('feature_lotus'))\
                                        .withColumn('mech_name', F.lit('ALL_Mech'))
      
      combined_kpi_all = sku_kpi_all.unionByName(brand_in_cate_kpi_all, allowMissingColumns=True)\
                                    .unionByName(all_category_kpi_all, allowMissingColumns=True)\
                                      .unionByName(all_lotus_kpi_all, allowMissingColumns=True)\
                                    .fillna(0)
                                    
      kpi_df = to_pandas(combined_kpi_final.unionByName(combined_kpi_all))
    
    df_pv = kpi_df[['period_eq_fis_wk', 'kpi_level', 'MyLo_customer','mech_name']].pivot(index='period_eq_fis_wk', columns=['kpi_level','mech_name'], values='MyLo_customer')
    
    #df_pv['%cust_sku_in_category']   = df_pv['feature_sku']/df_pv['feature_category']*100
    #df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    #df_pv['%cust_brand_in_category'] = df_pv['feature_brand']/df_pv['feature_category']*100
    #df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    
    mech_unique = kpi_df["mech_name"].unique()
    for i in mech_unique:
        df_pv[('%cust_sku_in_category', i)] = df_pv[('feature_sku', i)] / df_pv[('feature_category', i)] * 100
        df_pv[('%cust_brand_in_category', i)] = df_pv[('feature_brand', i)] / df_pv[('feature_category', i)] * 100
    
    df_pv.sort_index(ascending=False, inplace=True)
    
    cust_share_pd = df_pv.T.reset_index()
    
    return combined_kpi_final, kpi_df, cust_share_pd

# COMMAND ----------

### Scent Dev 2024 By Customer by mech 
def cust_kpi_noctrl_pm_dev_eql(txn
                    ,store_fmt
                    ,test_store_sf
                    ,feat_list
                    ,brand_df
                    ,cate_df
                    ):
    """Promo-eval : customer KPIs Pre-Dur for test store
    - Features SKUs
    - Feature Brand in subclass
    - Brand dataframe (all SKU of brand in category - switching level wither class/subclass)
    - Category dataframe (all SKU in category at defined switching level)
    - Week period
    - Return 
      >> combined_kpi : spark dataframe with all combine KPI
      >> kpi_df : combined_kpi in pandas
      >> df_pv : Pivot format of kpi_df
    """ 

    features_brand    = brand_df
    features_category = cate_df
    

    #test_store_sf:pyspark.sql.dataframe.DataFrame
    #store_id:integer
    #c_start:date
    #c_end:date
    #mech_count:integer
    #mech_name:string
    #media_fee_psto:double
    #aisle_subclass:string
    #aisle_scope:string
    
    kpis = [(F.countDistinct('date_id')/7).alias('n_weeks'),
            F.sum('net_spend_amt').alias('sales'),
            F.sum('pkg_weight_unit').alias('units'),
            F.countDistinct('transaction_uid').alias('visits'),
            F.countDistinct('household_id').alias('customer'),
           (F.sum('net_spend_amt')/F.countDistinct('household_id')).alias('spc'),
           (F.sum('net_spend_amt')/F.countDistinct('transaction_uid')).alias('spv'),
           (F.countDistinct('transaction_uid')/F.countDistinct('household_id')).alias('vpc'),
           (F.sum('pkg_weight_unit')/F.countDistinct('transaction_uid')).alias('upv'),
           (F.sum('net_spend_amt')/F.sum('pkg_weight_unit')).alias('ppu')]
        
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().groupBy("store_id").agg(F.concat_ws("_", F.collect_list("mech_name")).alias("mech_name")).dropDuplicates().persist()
      
      txn_test_pre = txn_all.filter(txn_all.period_eq_promo_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_eq_promo_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                            .withColumn("ALL", F.lit("ALL_Mech"))\
                            .dropDuplicates()
    else:
      trg_str_df   = test_store_sf.select('store_id',"mech_name").dropDuplicates().persist()
      txn_test_pre = txn_all.filter(txn_all.period_eq_promo_wk.isin(['pre']))\
                            .join  (trg_str_df , 'store_id', 'inner')\
                            .dropDuplicates()
                            
      txn_test_cmp = txn_all.filter(txn_all.period_eq_promo_wk.isin(['cmp']))\
                            .join(trg_str_df, 'store_id', 'inner')\
                            .dropDuplicates()

    txn_test_combine = \
    (txn_test_pre
     .unionByName(txn_test_cmp, allowMissingColumns=True)
     .withColumn('carded_nonCarded', F.when(F.col('household_id').isNotNull(), 'MyLo').otherwise('nonMyLo'))
    )
    
    sku_kpi = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                              .groupBy('period_eq_promo_wk',"mech_name")\
                              .pivot('carded_nonCarded')\
                              .agg(*kpis)\
                              .withColumn('kpi_level', F.lit('feature_sku'))
                              
    brand_in_cate_kpi = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                                .groupBy('period_eq_promo_wk',"mech_name")\
                                                .pivot('carded_nonCarded')\
                                                .agg(*kpis)\
                                                .withColumn('kpi_level', F.lit('feature_brand'))
        
        
    all_category_kpi = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                       .groupBy('period_eq_promo_wk',"mech_name")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_category'))

    all_lotus_kpi = txn_test_combine.groupBy('period_eq_promo_wk',"mech_name")\
                                       .pivot('carded_nonCarded')\
                                       .agg(*kpis)\
                                       .withColumn('kpi_level', F.lit('feature_lotus'))                                  
    
    combined_kpi_final = sku_kpi.unionByName(brand_in_cate_kpi, allowMissingColumns=True)\
                                .unionByName(all_category_kpi, allowMissingColumns=True)\
                                .unionByName(all_lotus_kpi, allowMissingColumns=True)\
                                .fillna(0)
    kpi_df = to_pandas(combined_kpi_final) 
    
    if len(test_store_sf.select("mech_name").dropDuplicates().collect()) > 1:
      sku_kpi_all = txn_test_combine.filter(F.col('upc_id').isin(feat_list))\
                                .groupBy('period_eq_promo_wk')\
                                .pivot('carded_nonCarded')\
                                .agg(*kpis)\
                                .withColumn('kpi_level', F.lit('feature_sku'))\
                                .withColumn('mech_name', F.lit('ALL_Mech'))
                                
      brand_in_cate_kpi_all = txn_test_combine.join(features_brand, 'upc_id', 'left_semi')\
                                              .groupBy('period_eq_promo_wk')\
                                              .pivot('carded_nonCarded')\
                                              .agg(*kpis)\
                                              .withColumn('kpi_level', F.lit('feature_brand'))\
                                              .withColumn('mech_name', F.lit('ALL_Mech'))
      
      
      all_category_kpi_all = txn_test_combine.join(features_category, 'upc_id', 'left_semi')\
                                        .groupBy('period_eq_promo_wk')\
                                        .pivot('carded_nonCarded')\
                                        .agg(*kpis)\
                                        .withColumn('kpi_level', F.lit('feature_category'))\
                                        .withColumn('mech_name', F.lit('ALL_Mech'))
      
      all_lotus_kpi_all = txn_test_combine.groupBy('period_eq_promo_wk')\
                                        .pivot('carded_nonCarded')\
                                        .agg(*kpis)\
                                        .withColumn('kpi_level', F.lit('feature_lotus'))\
                                        .withColumn('mech_name', F.lit('ALL_Mech'))
      
      combined_kpi_all = sku_kpi_all.unionByName(brand_in_cate_kpi_all, allowMissingColumns=True)\
                                    .unionByName(all_category_kpi_all, allowMissingColumns=True)\
                                    .unionByName(all_lotus_kpi_all, allowMissingColumns=True)\
                                    .fillna(0)
      kpi_df = to_pandas(combined_kpi_final.unionByName(combined_kpi_all))
    
    df_pv = kpi_df[['period_eq_promo_wk', 'kpi_level', 'MyLo_customer','mech_name']].pivot(index='period_eq_promo_wk', columns=['kpi_level','mech_name'], values='MyLo_customer')
    
    #df_pv['%cust_sku_in_category']   = df_pv['feature_sku']/df_pv['feature_category']*100
    #df_pv['%cust_sku_in_class'] = df_pv['feature_sku']/df_pv['feature_class']*100
    #df_pv['%cust_brand_in_category'] = df_pv['feature_brand']/df_pv['feature_category']*100
    #df_pv['%cust_brand_in_class'] = df_pv['feature_brand_in_subclass']/df_pv['feature_subclass']*100
    
    mech_unique = kpi_df["mech_name"].unique()
    for i in mech_unique:
        df_pv[('%cust_sku_in_category', i)] = df_pv[('feature_sku', i)] / df_pv[('feature_category', i)] * 100
        df_pv[('%cust_brand_in_category', i)] = df_pv[('feature_brand', i)] / df_pv[('feature_category', i)] * 100
    
    df_pv.sort_index(ascending=False, inplace=True)
    
    cust_share_pd = df_pv.T.reset_index()
    
    return combined_kpi_final, kpi_df, cust_share_pd

# COMMAND ----------


