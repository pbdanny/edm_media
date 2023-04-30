import pprint
from ast import literal_eval
from typing import List
from datetime import datetime, timedelta
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval

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
    txn = cmp.txn
    cp_start_date = cmp.cmp_start
    cp_end_date = cmp.cmp_end
    wk_type = cmp.wk_type
    test_store_sf = cmp.target_store
    adj_prod_sf = cmp.aisle_sku
    brand_sf = cmp.feat_brand_sku
    feat_sf = cmp.feat_sku
    
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')

    #--- Helper fn
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
    period_wk_col = load_config.get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    # Brand activate
    target_str             = load_config.get_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
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
