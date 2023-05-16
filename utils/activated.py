import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta
import sys
import os

import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

from .DBPath import DBPath
from .campaign_config import CampaignEval
from .exposure import create_txn_x_store_mech


def _get_period_wk_col_nm(cmp: CampaignEval) -> str:
        """Column name for period week identification
        """
        if cmp.wk_type in ["promo_week", "promo_wk"]:
            period_wk_col_nm = "period_promo_wk"
        elif cmp.wk_type in ["promozone"]:
            period_wk_col_nm = "period_promo_mv_wk"
        else:
            period_wk_col_nm = "period_fis_wk"
            
        return period_wk_col_nm

def get_cust_exposed(cmp: CampaignEval):
    create_txn_x_store_mech(cmp)
    cmp.cust_exposed = \
        (cmp.txn_x_store_mech
         .where(F.col("household_id").isNotNull())
         .groupBy("household_id")
         .select('household_id', 'transaction_uid', 'tran_datetime', 'mech_name')
         .drop_duplicates()
         .withColumnRenamed('transaction_uid', 'other_transaction_uid')
         .withColumnRenamed('tran_datetime', 'other_tran_datetime')
        )
        
    pass

def get_cust_shopper(cmp: CampaignEval,
                     sku: str):
    txn = cmp.txn
    cp_start_date = cmp.cmp_start
    cp_end_date = cmp.cmp_end
    wk_type = cmp.wk_type
    test_store_sf = cmp.target_store
    adj_prod_sf = cmp.aisle_sku
    brand_sf = cmp.feat_brand_sku
    feat_sf = cmp.feat_sku

    cmp.shopper = \
        (cmp.txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col_nm).isin(["cmp"]))
         .join(prd_scope_df, 'upc_id')
         .groupBy('household_id')
        .agg(F.min('date_id').alias('first_shp_date'))
        .drop_duplicates()
    )
        return out

    def _get_activated(exposed_cust: SparkDataFrame,
                       shppr_cust: SparkDataFrame
                       ) -> SparkDataFrame:
        """Get activated customer : First exposed date <= First (brand/sku) shopped date
        """
        out = \
            (exposed_cust.join(shppr_cust, "household_id", "left")
             .where(F.col('first_exposed_date').isNotNull())
             .where(F.col('first_shp_date').isNotNull())
             .where(F.col('first_exposed_date') <= F.col('first_shp_date'))
             .select( shppr_cust.household_id.alias('cust_id')
                     ,shppr_cust.first_shp_date.alias('first_shp_date')
                    )
             .drop_duplicates()
             )
        return out

    def _get_activated_sales(txn: SparkDataFrame
                            ,shppr_actv: SparkDataFrame
                            ,prd_scope_df : SparkDataFrame
                            ,prd_scope_nm : str
                            ,period_wk_col_nm: str
                            ):
        """ Get featured product's Sales values from activated customers (have seen media before buy product)
            return sales values of activated customers
        """
        txn_dur       = txn.where ( (F.col(period_wk_col_nm) == 'cmp') & (txn.household_id.isNotNull()) )

        cst_txn_dur   = txn_dur.join  ( prd_scope_df, txn_dur.upc_id == prd_scope_df.upc_id, 'left_semi')\
                               .join  ( shppr_actv,  txn_dur.household_id == shppr_actv.cust_id, 'inner')\
                               .select( txn_dur.date_id
                                       ,txn_dur.household_id
                                       ,shppr_actv.first_shp_date
                                       ,txn_dur.upc_id
                                       ,txn_dur.net_spend_amt.alias('sales_orig')
                                       ,F.when(txn_dur.date_id >= shppr_actv.first_shp_date, txn_dur.net_spend_amt)
                                         .when(txn_dur.date_id <  shppr_actv.first_shp_date, F.lit(0))
                                         .otherwise(F.lit(None))
                                         .alias('actv_sales')
                                       ,txn_dur.pkg_weight_unit.alias('pkg_weight_unit_orig')
                                       ,F.when(txn_dur.date_id >= shppr_actv.first_shp_date, txn_dur.pkg_weight_unit)
                                         .when(txn_dur.date_id <  shppr_actv.first_shp_date, F.lit(0))
                                         .otherwise(F.lit(None))
                                         .alias('actv_qty')
                                      )

        actv_sales_df     = cst_txn_dur.groupBy(cst_txn_dur.household_id)\
                                       .agg    ( F.max( cst_txn_dur.first_shp_date).alias('first_shp_date')
                                                ,F.sum( cst_txn_dur.actv_sales).alias('actv_spend')
                                                ,F.sum( cst_txn_dur.actv_qty).alias('actv_qty')
                                               )


        sum_actv_sales_df = actv_sales_df.agg( F.sum(F.lit(1)).alias(prd_scope_nm + '_activated_cust_cnt')
                                             , F.sum(actv_sales_df.actv_spend).alias(prd_scope_nm + '_actv_spend')
                                             , F.avg(actv_sales_df.actv_spend).alias(prd_scope_nm + '_avg_spc')
                                             , F.sum(actv_sales_df.actv_qty).alias(prd_scope_nm + '_actv_qty')
                                             , F.avg(actv_sales_df.actv_qty).alias(prd_scope_nm + '_avg_upc')
                                             )

        return actv_sales_df, sum_actv_sales_df

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

    # Brand activate
    target_str             = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
    cmp_exposed            = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)
    cmp_brand_shppr        = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=brand_sf)
    cmp_brand_activated    = _get_activated(exposed_cust=cmp_exposed, shppr_cust=cmp_brand_shppr)

    #nmbr_brand_activated  = cmp_brand_activated.count()
    #print(f'\n Total exposed and Feature Brand (in Category scope) shopper (Brand Activated) : {nmbr_brand_activated:,d}')

    brand_activated_info, brand_activated_sum =  _get_activated_sales( txn=txn
                                                                      , shppr_actv   = cmp_brand_activated
                                                                      , prd_scope_df = brand_sf
                                                                      , prd_scope_nm = 'brand'
                                                                      , period_wk_col_nm = period_wk_col)
    #nmbr_brand_activated  = brand_activated_sales
    #brand_sales_amt       = brand_activated_sales.collect()[0].actv_sales
    print('\n Total exposed and Feature Brand (in Category scope) shopper (Brand Activated) Display below' )

    brand_activated_sum.display()

    #print('\n Sales from exposed shopper (Brand Activated)                                  : ' + str(brand_sales_amt) + ' THB' )

    # Sku Activated
    cmp_sku_shppr         = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=feat_sf)
    cmp_sku_activated     = _get_activated(exposed_cust=cmp_exposed, shppr_cust=cmp_sku_shppr)

    #nmbr_sku_activated    = cmp_sku_activated.count()
    #print(f'\n Total exposed and Features SKU shopper (Features SKU Activated) : {nmbr_sku_activated:,d}')

    sku_activated_info, sku_activated_sum   =  _get_activated_sales( txn=txn
                                                                    , shppr_actv   = cmp_sku_activated
                                                                    , prd_scope_df = feat_sf
                                                                    , prd_scope_nm = 'sku'
                                                                    , period_wk_col_nm = period_wk_col)

    #sku_sales_amt         = sku_activated_sales.collect()[0].actv_sales

    print('\n Total exposed and Feature SKU shopper (SKU Activated) Display below' )

    sku_activated_sum.display()

    return brand_activated_info, sku_activated_info, brand_activated_sum, sku_activated_sum

def get_cust_activated_by_mech(cmp: CampaignEval,
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
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    cp_start_date = cmp.cmp_start
    cp_end_date = cmp.cmp_end
    wk_type = cmp.wk_type
    test_store_sf = cmp.target_store
    adj_prod_sf = cmp.aisle_sku
    brand_sf = cmp.feat_brand_sku
    feat_sf = cmp.feat_sku
    
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

def get_cust_movement(cmp: CampaignEval,
                      sku_activated: SparkDataFrame):
    """Customer movement based on tagged feature activated & brand activated

    """
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    wk_type = cmp.wk_type
    feat_sf = cmp.feat_sku
    class_df = cmp.feat_class_sku
    sclass_df = cmp.feat_subclass_sku
    brand_df = cmp.feat_brand_sku
    switching_lv = cmp.params["cate_lvl"]
      
    #---- Helper function
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

    #---- Main
    # Movement
    # Existing and New SKU buyer (movement at micro level)
    print("-"*80)
    print("Customer movement")
    print("Movement consider only Feature SKU activated")
    print("-"*80)

    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    # Features SKU movement
    prior_pre_sku_shopper = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))
     .join(feat_sf, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )

    existing_exposed_cust_and_sku_shopper = \
    (sku_activated
     .select("household_id")
     .join(prior_pre_sku_shopper, 'household_id', 'inner')
     .withColumn('customer_macro_flag', F.lit('existing'))
     .withColumn('customer_micro_flag', F.lit('existing_sku'))
     .checkpoint()
    )

    new_exposed_cust_and_sku_shopper = \
    (sku_activated
     .select("household_id")
     .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
     .withColumn('customer_macro_flag', F.lit('new'))
     .checkpoint()
    )

    # Customer movement for Feature SKU
    ## Macro level (New/Existing/Lapse)
    prior_pre_cc_txn = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()

    prior_pre_class_shopper = \
    (prior_pre_cc_txn
     .join(class_df, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )

    prior_pre_subclass_shopper = \
    (prior_pre_cc_txn
     .join(sclass_df, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )

    ## Micro level
    new_sku_new_store = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
    )

    new_sku_new_class = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'inner')
     .join(prior_pre_class_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_class'))
    )

    if switching_lv == 'subclass':
        new_sku_new_subclass = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
        )

        prior_pre_brand_in_subclass_shopper = \
        (prior_pre_cc_txn
         .join(sclass_df, "upc_id", "inner")
         .join(brand_df, "upc_id")
         .select('household_id')
         .drop_duplicates()
        )

        #---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )

        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )

        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_subclass)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
         .checkpoint()
        )

        return result_movement, new_exposed_cust_and_sku_shopper

    elif switching_lv == 'class':

        prior_pre_brand_in_class_shopper = \
        (prior_pre_cc_txn
         .join(class_df, "upc_id", "inner")
         .join(brand_df, "upc_id")
         .select('household_id')
        ).drop_duplicates()

        #---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )

        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )

        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
         .checkpoint()
        )

        return result_movement, new_exposed_cust_and_sku_shopper

    else:
        print('Not recognized Movement and Switching level param')
        return None, None

def get_cust_brand_switching_and_penetration(cmp: CampaignEval,
                                             cust_movement_sf: SparkDataFrame):
    """Media evaluation solution, customer switching
    """
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    wk_type = cmp.wk_type
    
    class_df = cmp.feat_class_sku
    sclass_df = cmp.feat_subclass_sku
    brand_df = cmp.feat_brand_sku
    switching_lv = cmp.params["cate_lvl"]
    
    #---- Helper fn
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

    ## Customer Switching by Sai
    def _switching(switching_lv:str, micro_flag: str, cust_movement_sf: SparkDataFrame,
                   prod_trans: SparkDataFrame, grp: List,
                   prod_lev: str, full_prod_lev: str ,
                   col_rename: str, period: str
                   ):
        """Customer switching from Sai
        """
        print(f'\t\t\t\t\t\t Switching of customer movement at : {micro_flag}')
        # List of customer movement at analysis micro level
        cust_micro_df = cust_movement_sf.where(F.col('customer_micro_flag') == micro_flag)
        prod_trans_cust_micro = prod_trans.join(cust_micro_df.select('household_id').dropDuplicates()
                                                , on='household_id', how='inner')
        cust_micro_kpi_prod_lv = \
        (prod_trans_cust_micro
         .groupby(grp)
         .agg(F.sum('net_spend_amt').alias('oth_'+prod_lev+'_spend'),
              F.countDistinct('household_id').alias('oth_'+prod_lev+'_customers'))
         .withColumnRenamed(col_rename, 'oth_'+full_prod_lev)
        )

        total_oth = \
        (cust_micro_kpi_prod_lv
         .agg(F.sum('oth_'+prod_lev+'_spend').alias('_total_oth_spend'))
        ).collect()[0][0]

        ## Add check None -- to prevent error Float (NoneType) --- Pat 25 Nov 2022
        if total_oth is None:
            total_oth = 0
        ## end if

        cust_micro_kpi_prod_lv = cust_micro_kpi_prod_lv.withColumn('total_oth_'+prod_lev+'_spend', F.lit(float(total_oth)))

        print("\t\t\t\t\t\t**Running micro df2")
        # Join micro df with prod trans
        if (prod_lev == 'brand') & (switching_lv == 'subclass'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name',
                      'class_name',
                      # 'subclass_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')

        elif (prod_lev == 'brand') & (switching_lv == 'class'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name',
                      'class_name', # TO BE DONE support for multi-class
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')

        #---- To be done : if switching at multi class
        # elif prod_lev == 'class':
        #     micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='section_name', how='inner')
        else:
            micro_df_summ = cmp.spark.createDataFrame([],[])

        print("\t\t\t\t\t\t**Running Summary of micro df")
        switching_result = \
        (micro_df_summ
         .select('division_name','department_name','section_name',
                 'class_name', # TO BE DONE support for multi-class
                 'original_brand',
                 'customer_macro_flag','customer_micro_flag','total_ori_brand_cust','total_ori_brand_spend',
                 'oth_'+full_prod_lev,'oth_'+prod_lev+'_customers','oth_'+prod_lev+'_spend','total_oth_'+prod_lev+'_spend')
         .withColumn('pct_cust_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_customers')/F.col('total_ori_brand_cust'))
         .withColumn('pct_spend_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_spend')/F.col('total_oth_'+prod_lev+'_spend'))
        #  .orderBy(F.col('pct_cust_oth_'+full_prod_lev).desc(),
                #   F.col('pct_spend_oth_'+full_prod_lev).desc()
        )

        switching_result = switching_result.checkpoint()

        return switching_result

    def _get_swtchng_pntrtn(switching_lv: str):
        """Get Switching and penetration based on defined switching at class / subclass
        Support multi subclass
        """
        if switching_lv == "subclass":
            prd_scope_df = sclass_df
            gr_col = ['division_name','department_name','section_name','class_name',
                      'brand_name','household_id']
        else:
            prd_scope_df = class_df
            gr_col = ['division_name','department_name','section_name',
                      "class_name",  # TO BE DONE support for multi subclass
                      'brand_name','household_id']

        prior_pre_cc_txn_prd_scope = \
        (txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col).isin(['pre', 'ppp']))
         .join(prd_scope_df, "upc_id", "inner")
        )

        prior_pre_cc_txn_prd_scope_sel_brand = prior_pre_cc_txn_prd_scope.join(brand_df, "upc_id", "inner")

        prior_pre_prd_scope_sel_brand_kpi = \
        (prior_pre_cc_txn_prd_scope_sel_brand
         .groupBy(gr_col)
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )

        dur_cc_txn_prd_scope = \
        (txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col).isin(['cmp']))
         .join(prd_scope_df, "upc_id", "inner")
        )

        dur_cc_txn_prd_scope_sel_brand = dur_cc_txn_prd_scope.join(brand_df, "upc_id", "inner")

        dur_prd_scope_sel_brand_kpi = \
        (dur_cc_txn_prd_scope_sel_brand
         .groupBy(gr_col)
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )

        pre_dur_band_spend = \
        (prior_pre_prd_scope_sel_brand_kpi
         .join(dur_prd_scope_sel_brand_kpi, gr_col, 'outer')
        )

        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand',
                                                 cust_movement_pre_dur_spend,
                                                 prior_pre_cc_txn_prd_scope,
                                               # ['subclass_name', 'brand_name'],
                                                 ["class_name", 'brand_name'], # TO BE DONE : support for multi-subclass
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within subclass
        dur_prd_scope_cust = dur_cc_txn_prd_scope.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_prd_scope
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(dur_prd_scope_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )

        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen

    #---- Main
    print("-"*80)
    print("Customer brand switching")
    print(f"Brand switching within : {switching_lv.upper()}")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    new_to_brand_switching, cust_mv_pre_dur_spend, brand_cust_pen = _get_swtchng_pntrtn(switching_lv=switching_lv)
    cust_brand_switching_and_pen = \
        (new_to_brand_switching.alias("a")
         .join(brand_cust_pen.alias("b"),
               F.col("a.oth_brand_in_category")==F.col("b.brand_name"), "left")
                  .orderBy(F.col("pct_cust_oth_brand_in_category").desc())
        )

    return new_to_brand_switching, brand_cust_pen, cust_brand_switching_and_pen

def get_cust_brand_switching_and_penetration_multi(cmp: CampaignEval,
                                                   cust_movement_sf: SparkDataFrame,
        ):
    """Media evaluation solution, customer switching
    """
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    wk_type = cmp.wk_type
    cate_df = cmp.feat_cate_sku
    switching_lv = cmp.params["cate_lvl"]
    wk_type = cmp.wk_type
    
    #---- Helper fn
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

    #---- Main
    print("-"*80)
    print("Customer brand switching")
    print(f"Brand switching within : {switching_lv.upper()}")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    new_to_brand_cust = cust_movement_sf.where(F.col('customer_micro_flag') == "new_to_brand")
    n_new_to_brand_cust = cust_movement_sf.where(F.col('customer_micro_flag') == "new_to_brand").agg(F.count_distinct("household_id")).collect()[0][0]

    prior_pre_new_to_brand_txn_in_cate = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))

     .join(new_to_brand_cust, "household_id", "inner")
     .join(cate_df, "upc_id", "inner")
    )
    prior_pre_new_to_brand_txn_in_cate.agg(F.count_distinct("household_id")).display()
    combine_hier = \
    (prior_pre_new_to_brand_txn_in_cate
     .select("brand_name", F.concat_ws("_", "division_name", "department_name", "section_name", "class_name", "subclass_name").alias("comb_hier"))
     .groupBy("brand_name")
     .agg(F.collect_set("comb_hier").alias("category"))
     .select("brand_name", "category")
    )

    pre_new_to_brand_cate_cust = prior_pre_new_to_brand_txn_in_cate.agg(F.count_distinct("household_id")).collect()[0][0]
    pre_brand_in_cate = \
    (prior_pre_new_to_brand_txn_in_cate
     .groupBy("brand_name")
     .agg(F.count_distinct("household_id").alias("pre_brand_switch_cust"))
     .withColumn("new_to_brand_cust", F.lit(n_new_to_brand_cust))
     .withColumn("prop_cust_switch", F.col("pre_brand_switch_cust")/F.col("new_to_brand_cust"))
    )
    #pre_brand_in_cate.display()

    prior_pre_txn_in_cate = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))
     .join(cate_df, "upc_id", "inner")
    )

    pre_cate_cust = prior_pre_txn_in_cate.agg(F.count_distinct("household_id")).collect()[0][0]
    pre_brand_cust_pen = \
    (prior_pre_txn_in_cate
     .groupBy("brand_name")
     .agg(F.count_distinct("household_id").alias("pre_brand_cust"))
     .withColumn("pre_total_cate_cust", F.lit(pre_cate_cust))
     .withColumn("cust_pen", F.col("pre_brand_cust")/F.col("pre_total_cate_cust"))
    )

    switch_pen = \
    (pre_brand_in_cate.join(pre_brand_cust_pen, "brand_name", "inner")
     .withColumn("switching_idx", F.col("prop_cust_switch")/F.col("cust_pen"))
     .join(combine_hier,  "brand_name", "inner")
     .orderBy(F.col("prop_cust_switch").desc_nulls_last())
     .withColumnRenamed("brand_name", "pre_brand_name")
    )
    # switch_pen.display()

    return switch_pen

def get_cust_sku_switching(cmp: CampaignEval,
                           sku_activated: SparkDataFrame):
    """Media evaluation solution, customer sku switching
    """
    cmp.spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    txn = cmp.txn
    wk_type = cmp.wk_type
    class_df = cmp.feat_class_sku
    sclass_df = cmp.feat_subclass_sku
    switching_lv = cmp.params["cate_lvl"]
    
    feat_list = cmp.feat_sku.toPandas()["upc_id"].to_numpy().tolist()

    #---- Helper fn
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

    #---- Main
    print("-"*80)
    print("Customer switching SKU for 'OFFLINE' + 'ONLINE'")
    print(f"Switching within : {switching_lv.upper()}")
    print("Customer Movement consider only Feature SKU activated")
    print("-"*80)

    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    if switching_lv == "subclass":
        cat_df = sclass_df
    else:
        cat_df = class_df

    prod_desc = cmp.spark.table('tdm.v_prod_dim_c').select('upc_id', 'product_en_desc').drop_duplicates()

    # (PPP+Pre) vs Dur Category Sales of each household_id
    ## Windows aggregate style
    txn_per_dur_cat_sale = \
        (txn
            .where(F.col('household_id').isNotNull())
            .where(F.col(period_wk_col).isin(['pre', 'ppp', 'cmp']))
            .join(cat_df, "upc_id", "inner")
            .withColumn('pre_cat_sales', F.when( F.col(period_wk_col).isin(['ppp', 'pre']) , F.col('net_spend_amt') ).otherwise(0) )
            .withColumn('dur_cat_sales', F.when( F.col(period_wk_col).isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
            .withColumn('cust_tt_pre_cat_sales', F.sum(F.col('pre_cat_sales')).over(Window.partitionBy('household_id') ))
            .withColumn('cust_tt_dur_cat_sales', F.sum(F.col('dur_cat_sales')).over(Window.partitionBy('household_id') ))
            )

    txn_cat_both = txn_per_dur_cat_sale.where( (F.col('cust_tt_pre_cat_sales')>0) & (F.col('cust_tt_dur_cat_sales')>0) )

    txn_cat_both_sku_only_dur = \
    (txn_cat_both
        .withColumn('pre_sku_sales',
                    F.when( (F.col(period_wk_col).isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
        .withColumn('dur_sku_sales',
                    F.when( (F.col(period_wk_col).isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
        .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
        .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
        .where( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
    )

    n_cust_switch_sku = \
    (txn_cat_both_sku_only_dur
        .where(F.col('pre_cat_sales')>0) # only other products
        .join(sku_activated, 'household_id', 'inner')
        .groupBy('upc_id')
        .agg(F.countDistinct('household_id').alias('custs'))
        .join(prod_desc, 'upc_id', 'left')
        .orderBy('custs', ascending=False)
    )

    return n_cust_switch_sku

def get_profile_truprice(cmp: CampaignEval,
                         sku_activated: SparkDataFrame):
    """Profile activated customer based on TruPrice segment
    Compare with total Lotus shopper at same store format
    """
    txn = cmp.txn
    wk_type = cmp.wk_type
    store_fmt = cmp.store_fmt
    cp_end_date = cmp.cmp_end
    class_df = cmp.feat_class_sku
    sclass_df = cmp.feat_subclass_sku
    switching_lv = cmp.params["cate_lvl"]
    
    #---- Helper fn
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

    def _get_truprice_seg(cp_end_date: str):
        """Get truprice seg from campaign end date
        With fallback period_id in case truPrice seg not available
        """
        from datetime import datetime, date, timedelta

        def __get_p_id(date_id: str,
                       bck_days: int = 0)-> str:
            """Get period_id for current or back date
            """
            date_dim = cmp.spark.table("tdm.v_date_dim")
            bck_date = (datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=bck_days)).strftime("%Y-%m-%d")
            bck_date_df = date_dim.where(F.col("date_id")==bck_date)
            bck_p_id = bck_date_df.select("period_id").drop_duplicates().collect()[0][0]

            return bck_p_id

        # Find period id to map Truprice / if the truprice period not publish yet use latest period
        bck_p_id = __get_p_id(cp_end_date, bck_days=180)
        truprice_all = \
            (cmp.spark.table("tdm_seg.srai_truprice_full_history")
             .where(F.col("period_id")>=bck_p_id)
             .select("household_id", "truprice_seg_desc", "period_id")
             .drop_duplicates()
            )
        max_trprc_p_id = truprice_all.agg(F.max("period_id")).drop_duplicates().collect()[0][0]

        crrnt_p_id = __get_p_id(cp_end_date, bck_days=0)

        if int(max_trprc_p_id) < int(crrnt_p_id):
            trprc_p_id = max_trprc_p_id
        else:
            trprc_p_id = crrnt_p_id

        trprc_seg = \
            (truprice_all
             .where(F.col("period_id")==trprc_p_id)
             .select("household_id", "truprice_seg_desc")
            )

        return trprc_seg, trprc_p_id

    def _get_truprice_cust_pen(txn: SparkDataFrame,
                               lv_nm: str):
        """Group by truprice , calculate cust penetration
        """
        tp_pen = \
            (txn
             .groupBy("truprice_seg_desc")
             .agg(F.countDistinct("household_id").alias(f"{lv_nm}_cust"))
             .withColumn(f"total_{lv_nm}_cust", F.sum(f"{lv_nm}_cust").over(Window.partitionBy()))
             .withColumn(f"{lv_nm}_cust_pen", F.col(f"{lv_nm}_cust")/F.col(f"total_{lv_nm}_cust"))
            )
        return tp_pen

    #---- Main
    print("-"*80)
    print("Profile activated customer : TruPrice")
    print(f"Index with Total Lotus & {switching_lv.upper()} shopper at format : {store_fmt.upper()}, OFFLINE + ONLINE")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)
    truprice_seg, truprice_period_id = _get_truprice_seg(cp_end_date=cp_end_date)
    print(f"TruPrice Segment Period Id : {truprice_period_id}")
    print("-"*80)

    # Map truprice at Lotus / prod scope
    if switching_lv == "subclass":
        prd_scope_df = sclass_df
    else:
        prd_scope_df = class_df

    txn_truprice = \
        (txn
         .where(F.col("household_id").isNotNull())
         .join(truprice_seg, "household_id", "left")
         .fillna(value="Unidentifed", subset=["truprice_seg_desc"])
         )
    txn_fmt = \
        (txn_truprice
         .where(F.col(period_wk_col).isin(["cmp"]))
         .where(F.upper(F.col("store_format_group"))==store_fmt.upper())
        )
    txn_fmt_prd_scp = txn_fmt.join(prd_scope_df, "upc_id")
    txn_actvtd = txn_fmt.join(sku_activated, "household_id", "inner")

    # Customer penetration by TruPrice
    fmt_tp = _get_truprice_cust_pen(txn_fmt, store_fmt.lower())
    prd_scp_tp = _get_truprice_cust_pen(txn_fmt_prd_scp, switching_lv.lower())
    actvtd_tp = _get_truprice_cust_pen(txn_actvtd, "sku_activated")
    combine_tp = fmt_tp.join(prd_scp_tp, "truprice_seg_desc", "left").join(actvtd_tp, "truprice_seg_desc", "left")

    # calculate index
    idx_tp = \
        (combine_tp
         .withColumn(f"idx_{store_fmt.lower()}", F.col(f"sku_activated_cust_pen")/F.col(f"{store_fmt.lower()}_cust_pen"))
         .withColumn(f"idx_{switching_lv.lower()}", F.col(f"sku_activated_cust_pen")/F.col(f"{switching_lv.lower()}_cust_pen"))
        )

    # Sort order by TruPrice
    df = idx_tp.toPandas()
    sort_dict = {"Most Price Insensitive": 0, "Price Insensitive": 1, "Price Neutral": 2, "Price Driven": 3, "Most Price Driven": 4, "Unidentifed": 5}
    df = df.sort_values(by=["truprice_seg_desc"], key=lambda x: x.map(sort_dict))  # type: ignore
    idx_tp = cmp.spark.createDataFrame(df)

    return idx_tp