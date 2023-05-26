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

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval
from utils import period_cal
from exposure.exposed import create_txn_offline_x_aisle_target_store

def get_cust_activated(cmp: CampaignEval):
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

    def _create_adj_prod_df(txn: SparkDataFrame) -> SparkDataFrame:
        """If adj_prod_sf is None, create from all upc_id in txn
        """
        out = txn.select("upc_id").drop_duplicates()
        return out

    def _get_exposed_cust(txn: SparkDataFrame,
                          test_store_sf: SparkDataFrame,
                          adj_prod_sf: SparkDataFrame,
                          channel: str = "OFFLINE"
                          ) -> SparkDataFrame:
        """Get exposed customer & first exposed date
        """
        # Changed filter column to offline_online_other_channel - Dec 2022 - Ta
        out = \
            (txn
             .where(F.col("offline_online_other_channel")==channel)
             .where(F.col("household_id").isNotNull())
             .join(test_store_sf, "store_id","inner") # Mapping cmp_start, cmp_end, mech_count by store
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
        out = txn.select("upc_id").drop_duplicates() #.checkpoint()
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

    # Get the "last seen" mechanic(s) that a shopper saw before they make purchaseds
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

    cmp_shppr_last_seen_brand_exposed_tag = cmp_shppr_last_seen_brand.withColumn('level', F.lit('brand')) \
                                                             .withColumn('total_mechanics_exposed',
                                                                         np.sum(cmp_shppr_last_seen_brand[col] for col in cmp_shppr_last_seen_brand.columns[1:num_of_mechanics+1]))
    
    print('Activated customers at brand level by number of mechanics exposed:')
    cmp_shppr_last_seen_brand_exposed_tag.groupBy('level').pivot('total_mechanics_exposed').agg(F.countDistinct(F.col('household_id'))).show()
    
    # Get activated customers at SKU level using "last seen" method
    cmp_shppr_last_seen_sku  = _get_activ_mech_last_seen(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf, 
                                                         period_wk_col=period_wk_col, prd_scope_df=feat_sf,
                                                         cp_start_date=cp_start_date, cp_end_date=cp_end_date,
                                                         promozone_flag=promozone_flag)
    
    cmp_shppr_last_seen_sku_exposed_tag = cmp_shppr_last_seen_sku.withColumn('level', F.lit('sku')) \
                                                         .withColumn('total_mechanics_exposed',
                                                                     np.sum(cmp_shppr_last_seen_sku[col] for col in cmp_shppr_last_seen_sku.columns[1:num_of_mechanics+1]))
    
    print('Activated customers at SKU level by number of mechanics exposed:')
    cmp_shppr_last_seen_sku_exposed_tag.groupBy('level').pivot('total_mechanics_exposed').agg(F.countDistinct(F.col('household_id'))).show()

    # Get numbers of activated customers for all mechanics for brand and SKU levels
    activated_brand_num = cmp_shppr_last_seen_brand_exposed_tag.groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                       .withColumn('mechanic', F.lit('all'))
    
    activated_sku_num = cmp_shppr_last_seen_sku_exposed_tag.groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                   .withColumn('mechanic', F.lit('all'))
    
    # Add by mech if there are more than 1 mechanic
    if num_of_mechanics > 1:
        mech_result_brand = {}
        mech_result_sku = {}
        
        for mech in mechanic_list:
            mech_result_brand[mech] = cmp_shppr_last_seen_brand_exposed_tag.filter(F.col(mech) == 1) \
                                                                   .groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                                   .withColumn('mechanic', F.lit(mech))
            
            mech_result_sku[mech] = cmp_shppr_last_seen_sku_exposed_tag.filter(F.col(mech) == 1) \
                                                               .groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                               .withColumn('mechanic', F.lit(mech))

            activated_brand_num = activated_brand_num.unionByName(mech_result_brand[mech])
            activated_sku_num = activated_sku_num.unionByName(mech_result_sku[mech])
            
    activated_both_num = activated_brand_num.unionByName(activated_sku_num).select('level', 'mechanic', 'num_activated')
    
    print('Number of customers activated by each mechanic: ')
    activated_both_num.show()

    return cmp_shppr_last_seen_brand_exposed_tag, cmp_shppr_last_seen_sku_exposed_tag, activated_both_num

#---- Exposure any mechnaics 
def get_cust_first_exposed_any_mech(cmp: CampaignEval):
    create_txn_offline_x_aisle_target_store(cmp)
    cmp.cust_first_exposed = \
        (cmp.txn_offline_x_aisle_target_store
         .where(F.col("household_id").isNotNull())
         .groupBy("household_id")
         .agg(F.min("date_id").alias("first_exposed_date"))
        )
    pass

def get_cust_first_prod_purchase_date(cmp: CampaignEval,
                                      prd_scope_df: SparkDataFrame):
        """Get first brand shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel
        """
        period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

        cmp.cust_first_prod_purchase = \
            (cmp.txn
             .where(F.col('household_id').isNotNull())
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(prd_scope_df, 'upc_id')
             .groupBy('household_id')
             .agg(F.min('date_id').alias('first_purchase_date'))
             .drop_duplicates()
            )
        pass

def get_cust_any_mech_activated(cmp: CampaignEval,
                      prd_scope_df: SparkDataFrame):
        
    get_cust_first_exposed_any_mech(cmp)
    get_cust_first_prod_purchase_date(cmp, prd_scope_df)
    
    cmp.cust_activated = \
        (cmp.cust_first_exposed
         .join(cmp.cust_first_prod_purchase, "household_id", "left")
             .where(F.col('first_exposed_date').isNotNull())
             .where(F.col('first_purchase_date').isNotNull())
             .where(F.col('first_exposed_date') <= F.col('first_purchase_date'))
             .select(F.col("household_id"),
                     F.col("first_purchase_date").alias('first_purchase_date')
                    )
             .drop_duplicates()
             )
    pass

def get_cust_any_mech_activated_sales(cmp: CampaignEval,
                                 prd_scope_df: SparkDataFrame,
                                 prd_scope_nm: str):
    
    get_cust_any_mech_activated(cmp, prd_scope_df)
    
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    txn_dur = \
        (cmp.txn
         .where(F.col(period_wk_col_nm).isin(["cmp"]))
         .where(F.col("household_id").isNotNull())
        )

    cst_txn_dur = txn_dur.join( prd_scope_df, "upc_id", 'left_semi')\
                                .join  ( cmp.cust_activated,  "household_id", 'inner')\
                                .select( txn_dur.date_id
                                        ,txn_dur.household_id
                                        ,cmp.cust_activated.first_purchase_date
                                        ,txn_dur.upc_id
                                        ,txn_dur.net_spend_amt.alias('sales_orig')
                                        ,F.when(txn_dur.date_id >= cmp.cust_activated.first_purchase_date, txn_dur.net_spend_amt)
                                            .when(txn_dur.date_id <  cmp.cust_activated.first_purchase_date, F.lit(0))
                                            .otherwise(F.lit(None))
                                            .alias('actv_sales')
                                        ,txn_dur.pkg_weight_unit.alias('pkg_weight_unit_orig')
                                        ,F.when(txn_dur.date_id >= cmp.cust_activated.first_purchase_date, txn_dur.pkg_weight_unit)
                                            .when(txn_dur.date_id <  cmp.cust_activated.first_purchase_date, F.lit(0))
                                            .otherwise(F.lit(None))
                                            .alias('actv_qty')
                                        )
    actv_sales_df     = cst_txn_dur.groupBy(cst_txn_dur.household_id)\
                                    .agg    ( F.max( cst_txn_dur.first_purchase_date).alias('first_purchase_date')
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

#---- Exposure by mechanics
def get_cust_all_exposed_by_mech(cmp: CampaignEval):
    create_txn_offline_x_aisle_target_store(cmp)
    cmp.cust_all_exposed = \
        (cmp.txn_offline_x_aisle_target_store
         .where(F.col("household_id").isNotNull())
         .select('household_id', 'transaction_uid', 'tran_datetime', 'mech_name', 'aisle_scope')
         .drop_duplicates()
         .withColumnRenamed('transaction_uid', 'exposed_transaction_uid')
         .withColumnRenamed('tran_datetime', 'exposed_tran_datetime')
        )
    pass

def get_cust_all_prod_purchase_date(cmp: CampaignEval,
                                prd_scope_df: SparkDataFrame):
        """Get all brand shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel
        """
        period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

        cmp.cust_all_prod_purchase = \
            (cmp.txn
             .where(F.col('household_id').isNotNull())
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(prd_scope_df, 'upc_id')
             .select('household_id', 'transaction_uid', 'tran_datetime', 'net_spend_amt', 'unit')
             .withColumnRenamed('transaction_uid', 'purchase_transaction_uid')
             .withColumnRenamed('tran_datetime', 'purchase_tran_datetime')
             .drop_duplicates()
            )
        pass

def get_cust_by_mech_last_seen_exposed_tag(cmp: CampaignEval,
                                 prd_scope_df: SparkDataFrame,
                                 prd_scope_nm: str):
    """
    """
    get_cust_all_exposed_by_mech(cmp)
    get_cust_all_prod_purchase_date(cmp, prd_scope_df)

    txn_each_purchase_most_recent_media_exposed = \
    (cmp.cust_all_exposed
    .join(cmp.cust_all_prod_purchase, on='household_id', how='inner')
    .where(F.col('exposed_tran_datetime') <= F.col('purchase_tran_datetime'))
    .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('exposed_tran_datetime'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('purchase_transaction_uid').orderBy(F.col('time_diff'))))
    .where(F.col('recency_rank') == 1).drop_duplicates()
    )
    purchased_exposure_count = \
        (txn_each_purchase_most_recent_media_exposed
        .groupBy('household_id', 'mech_name')
        .agg(F.count_distinct(F.col('purchase_transaction_uid')).alias("n_visit_purchased_exposure"))
        )

    purchased_exposure_n_mech = purchased_exposure_count.select("mech_name").drop_duplicates().count()

    purchased_exposure_flagged_pv = \
        (purchased_exposure_count
        .withColumn("exposure_flag", F.lit(1))
        .groupBy("household_id")
        .pivot("mech_name")
        .agg(F.first(F.col("exposure_flag")))
        .fillna(0)
        )

    total_purchased_exposure_flagged_by_cust = \
        (purchased_exposure_flagged_pv
         .withColumn('level', F.lit(prd_scope_nm))
         .withColumn('total_mechanics_exposed',
            sum(purchased_exposure_flagged_pv[col] for col in purchased_exposure_flagged_pv.columns[1:purchased_exposure_n_mech+1]))
         )
        
    return total_purchased_exposure_flagged_by_cust

def get_cust_by_mach_activated(cmp: CampaignEval):
    mechanic_list = cmp.target_store.select('mech_name').drop_duplicates().rdd.flatMap(lambda x: x).collect()
    num_of_mechanics = len(mechanic_list)

    cmp_shppr_last_seen_sku_exposed_tag = get_cust_by_mech_last_seen_exposed_tag(cmp, cmp.feat_sku, prd_scope_nm="sku")
    cmp_shppr_last_seen_brand_exposed_tag = get_cust_by_mech_last_seen_exposed_tag(cmp, cmp.feat_brand_sku, prd_scope_nm="brand")
    
    # Get numbers of activated customers for all mechanics for brand and SKU levels
    activated_brand_num = cmp_shppr_last_seen_brand_exposed_tag.groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                       .withColumn('mechanic', F.lit('all'))
    
    activated_sku_num = cmp_shppr_last_seen_sku_exposed_tag.groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                   .withColumn('mechanic', F.lit('all'))
    
    # Add by mech if there are more than 1 mechanic
    if num_of_mechanics > 1:
        mech_result_brand = {}
        mech_result_sku = {}
        
        for mech in mechanic_list:
            mech_result_brand[mech] = cmp_shppr_last_seen_brand_exposed_tag.filter(F.col(mech) == 1) \
                                                                   .groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                                   .withColumn('mechanic', F.lit(mech))
            
            mech_result_sku[mech] = cmp_shppr_last_seen_sku_exposed_tag.filter(F.col(mech) == 1) \
                                                               .groupBy('level').agg(F.countDistinct(F.col('household_id')).alias('num_activated')) \
                                                               .withColumn('mechanic', F.lit(mech))

            activated_brand_num = activated_brand_num.unionByName(mech_result_brand[mech])
            activated_sku_num = activated_sku_num.unionByName(mech_result_sku[mech])
            
    activated_both_num = activated_brand_num.unionByName(activated_sku_num).select('level', 'mechanic', 'num_activated')
    
    print('Number of customers activated by each mechanic: ')
    activated_both_num.show()

    return cmp_shppr_last_seen_brand_exposed_tag, cmp_shppr_last_seen_sku_exposed_tag, activated_both_num

#---- Cross categoy exposure
def get_bask_by_aisle_scope_last_seen(cmp: CampaignEval,
                                          prd_scope_df: SparkDataFrame,
                                          prd_scope_nm: str):
    """
    """
    get_cust_all_exposed_by_mech(cmp)
    get_cust_all_prod_purchase_date(cmp, prd_scope_df)

    txn_each_purchase_most_recent_media_exposed = \
    (cmp.cust_all_exposed
    .join(cmp.cust_all_prod_purchase, on='household_id', how='inner')
    .where(F.col('exposed_tran_datetime') <= F.col('purchase_tran_datetime'))
    .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('exposed_tran_datetime'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('purchase_transaction_uid').orderBy(F.col('time_diff'))))
    .where(F.col('recency_rank') == 1)
    .drop_duplicates()
    .select('household_id', 'exposed_transaction_uid', 'mech_name','aisle_scope','purchase_transaction_uid', "net_spend_amt", "unit")
    )
        
    return txn_each_purchase_most_recent_media_exposed