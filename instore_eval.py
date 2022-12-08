from copy import deepcopy
from datetime import datetime, timedelta
import functools
from typing import List

import sys
import os

import pandas as pd
import numpy as np
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

from pyspark.dbutils import DBUtils

spark = SparkSession.builder.appName("media_eval").getOrCreate()
dbutils = DBUtils(spark)

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
from edm_helper import get_lag_wk_id, to_pandas

def print_dev(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print("+"*80)
        print("THIS IS DEV PART")
        print("+"*80)
        return func(*args, **kwargs)
        # print('Print after function run')
    return wrapper

def check_combine_region(store_format_group: str,
                         test_store_sf: SparkDataFrame,
                         txn: SparkDataFrame):
    """Base on store group name,
    - if HDE / Talad -> count check test vs total store
    - if GoFresh -> adjust 'store_region' in txn, count check
    """
    from typing import List
    from pyspark.sql import DataFrame as SparkDataFrame

    print('-'*80)
    print('Count check store region & Combine store region for GoFresh')
    print(f'Store format defined : {store_format_group}')

    def _get_all_and_test_store(str_fmt_id: List,
                                str_fmt_gr_nm: str,
                                test_store_sf: SparkDataFrame ):
        """Get universe store count, based on format definded
        If store region Null -> Not show in count
        """
        all_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .filter(F.col('format_id').isin(str_fmt_id))
         .select('store_id', 'store_name', F.col('region').alias('store_region')).drop_duplicates()
         .dropna('all', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'total_{str_fmt_gr_nm}'))
        )

        test_store_count_region = \
        (spark.table('tdm.v_store_dim')
         .select('store_id','store_name',F.col('region').alias('store_region')).drop_duplicates()
         .join(test_store_sf, 'store_id', 'left_semi')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'test_store_count'))
        )

        return all_store_count_region, test_store_count_region

    if store_format_group == 'hde':
        format_id_list = [1,2,3]
        all_store_count_region, test_store_count_region = _get_all_and_test_store(format_id_list, store_format_group, test_store_sf)

    elif store_format_group == 'talad':
        format_id_list = [4]
        all_store_count_region, test_store_count_region = _get_all_and_test_store(format_id_list, store_format_group, test_store_sf)

    elif store_format_group == 'gofresh':
        format_id_list = [5]

        #---- Adjust Transaction
        print('GoFresh : Combine store_region West + Central in variable "txn_all"')
        print("GoFresh : Auto-remove 'Null' region")

        adjusted_store_region =  \
        (spark.table('tdm.v_store_dim')
         .withColumn('store_region', F.when(F.col('region').isin(['West','Central']), F.lit('West+Central'))
                                      .when(F.col('region').isNull(), F.lit('Unidentified'))
                                      .otherwise(F.col('region')))
         .drop("region")
         .drop_duplicates()
        )

        #txn = txn.where(F.col("store_region").isNotNull()).drop('store_region').join(adjusted_store_region, 'store_id', 'left')
        txn = txn.drop('store_region').join(adjusted_store_region, 'store_id', 'left')

        #---- Count Region
        all_store_count_region = \
        (adjusted_store_region
         .filter(F.col('format_id').isin(format_id_list))
         .select('store_id', 'store_name', 'store_region')
         .drop_duplicates()
         .dropna('all', subset='store_region')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'total_{store_format_group}'))
        )

        test_store_count_region = \
        (adjusted_store_region
         .filter(F.col('format_id').isin(format_id_list))
         .select('store_id', 'store_name', 'store_region')
         .drop_duplicates()
         .dropna('all', subset='store_region')
         .join(test_store_sf, 'store_id', 'left_semi')
         .groupBy('store_region')
         .agg(F.count('store_id').alias(f'test_store_count'))
        )

    else:
        print(f'Unknown store format group name : {store_format_group}')
        return None, txn

    test_vs_all_store_count = all_store_count_region.join(test_store_count_region, 'store_region', 'left').orderBy('store_region')

    return test_vs_all_store_count, txn

def get_cust_activated(txn: SparkDataFrame,
                       cp_start_date: str,
                       cp_end_date: str,
                       wk_type: str,
                       test_store_sf: SparkDataFrame,
                       adj_prod_sf: SparkDataFrame,
                       brand_sf: SparkDataFrame,
                       feat_sf: SparkDataFrame):
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

def get_cust_movement(txn: SparkDataFrame,
                      wk_type: str,
                      feat_sf: SparkDataFrame,
                      sku_activated: SparkDataFrame,
                      class_df: SparkDataFrame,
                      sclass_df: SparkDataFrame,
                      brand_df: SparkDataFrame,
                      switching_lv: str
                      ):
    """Customer movement based on tagged feature activated & brand activated

    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper function
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

def get_cust_brand_switching_and_penetration(
        txn: SparkDataFrame,
        switching_lv: str,
        brand_df: SparkDataFrame,
        class_df: SparkDataFrame,
        sclass_df: SparkDataFrame,
        cust_movement_sf: SparkDataFrame,
        wk_type: str,
        ):
    """Media evaluation solution, customer switching
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper fn
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
            micro_df_summ = spark.createDataFrame([],[])

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

@print_dev
def get_cust_brand_switching_and_penetration_multi(
        txn: SparkDataFrame,
        switching_lv: str,
        brand_df: SparkDataFrame,
        class_df: SparkDataFrame,
        sclass_df: SparkDataFrame,
        cate_df: SparkDataFrame,
        cust_movement_sf: SparkDataFrame,
        wk_type: str,
        ):
    """Media evaluation solution, customer switching
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper fn
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

def get_cust_sku_switching(
        txn: SparkDataFrame,
        switching_lv: str,
        sku_activated: SparkDataFrame,
        feat_list: List,
        class_df: SparkDataFrame,
        sclass_df: SparkDataFrame,
        wk_type: str,
        ):
    """Media evaluation solution, customer sku switching
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper fn
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

    prod_desc = spark.table('tdm.v_prod_dim_c').select('upc_id', 'product_en_desc').drop_duplicates()

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

def get_profile_truprice(txn: SparkDataFrame,
                         store_fmt: str,
                         cp_end_date: str,
                         sku_activated: SparkDataFrame,
                         switching_lv: str,
                         class_df: SparkDataFrame,
                         sclass_df: SparkDataFrame,
                         wk_type: str,
                         ):
    """Profile activated customer based on TruPrice segment
    Compare with total Lotus shopper at same store format
    """
    #---- Helper fn
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

    def _get_truprice_seg(cp_end_date: str):
        """Get truprice seg from campaign end date
        With fallback period_id in case truPrice seg not available
        """
        from datetime import datetime, date, timedelta

        def __get_p_id(date_id: str,
                       bck_days: int = 0)-> str:
            """Get period_id for current or back date
            """
            date_dim = spark.table("tdm.v_date_dim")
            bck_date = (datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=bck_days)).strftime("%Y-%m-%d")
            bck_date_df = date_dim.where(F.col("date_id")==bck_date)
            bck_p_id = bck_date_df.select("period_id").drop_duplicates().collect()[0][0]

            return bck_p_id

        # Find period id to map Truprice / if the truprice period not publish yet use latest period
        bck_p_id = __get_p_id(cp_end_date, bck_days=180)
        truprice_all = \
            (spark.table("tdm_seg.srai_truprice_full_history")
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
    idx_tp = spark.createDataFrame(df)

    return idx_tp

@print_dev
def get_store_matching(txn: SparkDataFrame,
                       pre_en_wk: int,
                       wk_type: str,
                       feat_sf: SparkDataFrame,
                       brand_df: SparkDataFrame,
                       sclass_df: SparkDataFrame,
                       test_store_sf: SparkDataFrame,
                       reserved_store_sf: SparkDataFrame,
                       matching_methodology: str = 'varience') -> List:
    """
    Parameters
    ----------
    txn: SparkDataFrame
        
    pre_en_wk: End pre_period week --> yyyymm
        Pre period end week
        
    wk_type: "fis_week" or "promo_week"
    
    feat_sf: SparkDataFrame
        Features upc_id
        
    brand_df: SparkDataFrame
        Feature brand upc_id (brand in switching level)

    sclass_df: SparkDataFrame
        Featurs subclass upc_id (subclass in switching level)
        
    test_store_sf: SparkDataFrame
        Media features store list
        
    reserved_store_sf: SparkDataFrame
        Customer picked reserved store list, for finding store matching -> control store
        
    matching_methodology: str, default 'varience'
        'variance', 'euclidean', 'cosine_similarity'
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    
    from sklearn.preprocessing import StandardScaler

    from scipy.spatial import distance
    import statistics as stats
    from sklearn.metrics.pairwise import cosine_similarity
    
    #---- Helper fn
    def _get_wk_id_col_nm(wk_type: str
                              ) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo_week"]:
            wk_id_col_nm = "promoweek_id"
        elif wk_type in ["promozone"]:
            wk_id_col_nm = "promoweek_id"
        else:
            wk_id_col_nm = "week_id"
            
        return wk_id_col_nm
    
    def _get_min_wk_sales(prod_scope_df: SparkDataFrame):
        """Count number of week sales by store, return the smallest number of target store, control store
        """
        # Min sales week test store
        txn_match_trg  = (txn
                          .join(test_store_sf.drop("store_region_orig", "store_region"), "store_id", "inner")
                          .join(prod_scope_df, "upc_id", "leftsemi")
                          .where(F.col(wk_id_col_nm).between(pre_st_wk, pre_en_wk))
                          .where(F.col("offline_online_other_channel") == 'OFFLINE')
                          .select(txn['*'],
                                F.col("store_region").alias('store_region_new'),
                                F.lit('test').alias('store_type'),
                                F.col("mech_name").alias('store_mech_set'))
                          )
        trg_wk_cnt_df = txn_match_trg.groupBy(F.col("store_id")).agg(F.count_distinct(F.col(wk_id_col_nm)).alias('wk_sales'))
        trg_min_wk = trg_wk_cnt_df.agg(F.min(trg_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0][0]
        
        # Min sale week ctrl store
        txn_match_ctl = (txn
                         .join(reserved_store_sf.drop("store_region_orig", "store_region"), "store_id", 'inner')
                         .join(prod_scope_df, "upc_id", "leftsemi")
                         .where(F.col(wk_id_col_nm).between(pre_st_wk, pre_en_wk))
                         .where(F.col("offline_online_other_channel") == 'OFFLINE')
                         .select(txn['*'],
                                 F.col("store_region").alias('store_region_new'),
                                 F.lit('ctrl').alias('store_type'),
                                 F.lit('No Media').alias('store_mech_set'))
                         )
                            
        ctl_wk_cnt_df = txn_match_ctl.groupBy("store_id").agg(F.count_distinct(F.col(wk_id_col_nm)).alias('wk_sales'))
        ctl_min_wk = ctl_wk_cnt_df.agg(F.min(ctl_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0][0]
        
        return int(trg_min_wk), txn_match_trg, int(ctl_min_wk), txn_match_ctl
    
    def _get_comp_score(txn: SparkDataFrame,
                        wk_id_col_nm: str):
        """Calculate weekly kpi by store_id
        """        
        def __get_std(df: PandasDataFrame) -> PandasDataFrame:
            """
            """
            from sklearn.preprocessing import StandardScaler, MinMaxScaler
            
            scalar = MinMaxScaler() # StandardScaler()
            
            scaled = scalar.fit_transform(df)
            scaled_df = pd.DataFrame(data=scaled, index=df.index, columns=df.columns)
            
            return scaled_df
        
        txn = txn.withColumn("store_id", F.col("store_id").cast(StringType()))
        
        sales = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
        custs = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.count_distinct('household_id').alias('custs')).fillna(0)
        
        sales_df = to_pandas(sales).astype({'store_id':str}).set_index("store_id")
        custs_df = to_pandas(custs).astype({'store_id':str}).set_index("store_id")
        
        sales_scaled_df = __get_std(sales_df)
        custs_scaled_df = __get_std(custs_df)
        
        sales_unpv_df = sales_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_sales", var_name="week_id")
        custs_unpv_df = custs_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_custs", var_name="week_id")        
        comb_df = pd.merge(sales_unpv_df, custs_unpv_df, how="outer", on=["store_id", "week_id"])
        comb_df["comp_score"] = (comb_df["std_sales"] + comb_df["std_custs"])/2
        
        return comb_df
    
    #--------------        
    #---- Main ----
    #--------------
    
    print("-"*80)
    wk_id_col_nm = _get_wk_id_col_nm(wk_type=wk_type)
    print(f"Week_id based on column {wk_id_col_nm}")
    print(' Matching performance only "OFFLINE" \n ' + '-'*80 + '\n')
    print("-"*80)
    
    pre_st_wk  = get_lag_wk_id(wk_id=pre_en_wk, lag_num=13, inclusive=True)
    # Test min week features with sales >= 3 wks
    trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(feat_sf)
    
    if (trg_min_wk >= 3) & (ctl_min_wk >= 3):
        match_lvl = 'feature sku'
        txn_matching = txn_match_trg.union(txn_match_ctl)
    else:
        trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(brand_df)
        if (trg_min_wk >= 3) & (ctl_min_wk >= 3):
            match_lvl = 'brand'
            txn_matching = txn_match_trg.union(txn_match_ctl)   
        else:
            trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(sclass_df)
            match_lvl = 'subclass'
            txn_matching = txn_match_trg.union(txn_match_ctl)
    
    print("-"*80)
    print(f'This campaign will do matching at "{match_lvl.upper()}"\n')
    print("-"*80)
    
    # Get composite score by store
    store_comp_score = _get_comp_score(txn_matching, wk_id_col_nm)
    store_comp_score_pv = store_comp_score.pivot(index="store_id", columns="week_id", values="comp_score").reset_index()
    
    # get store_id, store_region_new, store_type, store_mech_set
    store_type = txn_matching.select(F.col("store_id").cast(StringType()), "store_region_new", "store_type", "store_mech_set").drop_duplicates().toPandas()
    region_list = store_type["store_region_new"].dropna().unique()

    # setup dict to collect info
    dist_dict = {}
    var_dict = {}
    cos_dict = {}
    
    # Loop in each region
    for r in region_list:
        print(r)
        # List of store_id in those region for test, ctrl
        test_store_id = store_type[(store_type["store_region_new"]==r) & (store_type["store_type"]=="test")]["store_id"]
        ctrl_store_id = store_type[(store_type["store_region_new"]==r) & (store_type["store_type"]=="ctrl")]["store_id"]
        
        # Store_id and score for test, ctrl
        test_store_score = store_comp_score_pv[store_comp_score_pv["store_id"].isin(test_store_id)].reset_index()
        ctrl_store_score = store_comp_score_pv[store_comp_score_pv["store_id"].isin(ctrl_store_id)].reset_index()
        
        display(test_store_score)
        display(ctrl_store_score)
        
        # Loop test store score, only region with test store
        if len(test_store_score) > 0:
            for i in range(len(test_store_score)):

                #set standard euc_distance
                dist0 = 10**9
                #set standard var
                var0 = 100
                #set standard cosine
                cos0 = -1

                # finding its region & store_id
                test_store_id = test_store_score.iloc[i].store_id
                
                # get value from that test store
                test_i_score = test_store_score[i]

                # get index for reserved store
                ctr_index = ctrl_store_score.index

                # Loop ctrl store
                for j in ctr_index:
                    
                    ctrl_i_score = ctrl_store_score[j]
                    res_store_id = ctrl_store_score.iloc[j].store_id

            #-----------------------------------------------------------------------------
                    #finding min distance
                    dist = distance.euclidean(test_i_score, ctrl_i_score)
                    if dist < dist0:
                        dist0 = dist
                        dist_dict[test_store_id] = [res_store_id, dist]

            #-----------------------------------------------------------------------------            
                    #finding min var
                    var = stats.variance(np.abs(test_i_score - ctrl_i_score))
                    if var < var0:
                        var0 = var
                        var_dict[test_store_id] = [res_store_id, var]

            #-----------------------------------------------------------------------------  
                    #finding highest cos
                    cos = cosine_similarity(test_i_score.reshape(1,-1), ctrl_i_score.reshape(1,-1))[0][0]
                    if cos > cos0:
                        cos0 = cos
                        cos_dict[test_store_id] = [res_store_id,cos]
    
    #---- create dataframe            
    dist_df = pd.DataFrame(dist_dict,index=['ctr_store_dist','euc_dist']).T.reset_index().rename(columns={'index':'store_id'})
    var_df = pd.DataFrame(var_dict,index=['ctr_store_var','var']).T.reset_index().rename(columns={'index':'store_id'})
    cos_df = pd.DataFrame(cos_dict,index=['ctr_store_cos','cos']).T.reset_index().rename(columns={'index':'store_id'})
    
    ## join to have ctr store by each method
    ## Add 'store_mech_set' to test_df -->  Pat 6 Sep 2022
    
    matching_df = store_type.merge(dist_df[['store_id','ctr_store_dist']], on='store_id', how='left')\
                            .merge(var_df[['store_id','ctr_store_var']], on='store_id', how='left')\
                            .merge(cos_df[['store_id','ctr_store_cos']],on='store_id', how='left')

    #change data type to int
    matching_df.ctr_store_dist = matching_df.ctr_store_dist.astype('int')
    matching_df.ctr_store_var = matching_df.ctr_store_var.astype('int')
    matching_df.ctr_store_cos = matching_df.ctr_store_cos.astype('int')
    
    matching_df.rename(columns = {'store_region_new' : 'store_region'}, inplace = True)
    
    print(' \n Result matching table show below \n')
    matching_df.display()
    
    #----select control store using var method
    if matching_methodology == 'varience':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_var]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'euclidean':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_dist]))
        return ctr_store_list, matching_df
    elif matching_methodology == 'cosine_similarity':
        ctr_store_list = list(set([s for s in matching_df.ctr_store_cos]))
        return ctr_store_list, matching_df
    else:
        print('Matching metodology not in scope list : varience, euclidean, cosine_similarity')
        return None

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
    """
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

@print_dev
def get_customer_uplift_by_mech(txn: SparkDataFrame,
                       cp_start_date: str,
                       cp_end_date: str,
                       wk_type: str,
                       test_store_sf: SparkDataFrame,
                       adj_prod_sf: SparkDataFrame,
                       brand_sf: SparkDataFrame,
                       feat_sf: SparkDataFrame,
                       ctr_store_list: List,
                       cust_uplift_lv: str):
    """
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
            .withColumn("mech_name", F.lit("ctrl_store"))
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
             .join(test_store_sf, "store_id", "inner")  # Mapping cmp_start, cmp_end, mech_count, mech_name by store
             .join(adj_prod_sf, "upc_id", "inner")
             .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
             .select("household_id", "mech_name",
                     F.col("transaction_uid").alias("exposed_txn_id"),
                     F.col("tran_datetime").alias("exposed_datetime"))
             .withColumn("first_exposed_date", F.min(F.to_date("exposed_datetime")).over(Window.partitionBy("household_id")) )
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
             .select('household_id',
                     F.col("transaction_uid").alias("shp_txn_id"),
                     F.col("tran_datetime").alias("shp_datetime"))
             .withColumn("first_shp_date", F.min(F.to_date("shp_datetime")).over(Window.partitionBy("household_id")) )
         .drop_duplicates()
            )
        return out

    def _get_activated(exposed_cust: SparkDataFrame,
                       shppr_cust: SparkDataFrame
                       ) -> SparkDataFrame:
        """Get activated customer : First exposed date <= First (brand/sku) shopped date
        """
        out = \
            (exposed_cust
             .join(shppr_cust, "household_id", "left")
             .withColumn("sec_diff", F.col("shp_datetime").cast("long") - F.col("exposed_datetime").cast("long"))
             .withColumn("day_diff", F.datediff("shp_datetime", "exposed_datetime"))

             .where(F.col('first_exposed_date').isNotNull())
             .where(F.col('first_shp_date').isNotNull())
             .where(F.col('first_exposed_date') <= F.col('first_shp_date'))

             # new logic for multi mech
             .where(F.col("day_diff").isNotNull())
             .where(F.col("day_diff")>=0)
             .withColumn("proximity_rank",
                         F.row_number().over(Window
                                             .partitionBy("household_id", "shp_txn_id")
                                             .orderBy(F.col("day_diff").asc_nulls_last())))
             .where(F.col("proximity_rank")==1)
             .select("household_id", "mech_name")
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
    cmp_exposed_by_mech = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)
    cmp_brand_shppr = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=brand_sf)
    cmp_brand_activated_by_mech = _get_activated(exposed_cust=cmp_exposed_by_mech, shppr_cust=cmp_brand_shppr)

    ctr_str = _create_ctrl_store_sf(ctr_store_list=ctr_store_list, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
    cmp_unexposed = _get_exposed_cust(txn=txn, test_store_sf=ctr_str, adj_prod_sf=adj_prod_sf)
    cmp_unexposed_activated = \
        (_get_activated(exposed_cust=cmp_unexposed, shppr_cust=cmp_brand_shppr)
         .withColumnRenamed("exposed_datetime", "unexposed_datetime")
         .withColumnRenamed("exposed_txn_id", "unexposed_txn_id")
         .withColumnRenamed("first_exposed_date", "first_unexposed_date")
         )

    mech_nm_list = cmp_brand_activated_by_mech.select("mech_name").drop_duplicates().toPandas()["mech_name"].to_numpy().tolist()
    print("List of media mech for uplift calculation", mech_nm_list)

    out_df = spark.createDataFrame([], T.StructType([]))

    # Loop each mech_name
    for mech_nm in mech_nm_list:
        print("-"*40)
        print(f"Uplift for media mechanic : {mech_nm}")
        cmp_exposed = cmp_exposed_by_mech.where(F.col("mech_name")==mech_nm).select("household_id").drop_duplicates().withColumn("exposed_flag", F.lit(1))
        exposed_buy = cmp_brand_activated_by_mech.where(F.col("mech_name")==mech_nm).select("household_id").drop_duplicates().withColumn("exposed_and_buy_flag", F.lit(1))

        exposed_buy_flag = (cmp_exposed
                            .join(exposed_buy, "household_id", "left")
                            .fillna(0, subset=["exposed_and_buy_flag"])
                            )

        unexposed_buy = cmp_unexposed_activated.select("household_id").drop_duplicates().withColumn("unexposed_and_buy_flag", F.lit(1))
        unexposed_buy_flag = \
            (cmp_unexposed.select("household_id").drop_duplicates()
             .withColumn("unexposed_flag", F.lit(1))
             .join(unexposed_buy, "household_id", "left")
             .fillna(0, subset=["unexposed_and_buy_flag"])
             )

        exposed_unexposed_buy_flag = exposed_buy_flag.join(unexposed_buy_flag, "household_id", "outer").fillna(0)

        exposed_unexposed_buy_flag.groupBy('exposed_flag', 'unexposed_flag','exposed_and_buy_flag','unexposed_and_buy_flag').count().display()

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

        movement_and_exposure.where(F.col('exposed_flag')==1).groupBy('customer_group').agg(F.countDistinct('household_id')).display()

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
        uplift_out = spark.createDataFrame(df).withColumn("mech_name", F.lit(mech_nm))

        out_df = out_df.unionByName(uplift_out, allowMissingColumns=True)

    return out_df

# Comment out function to clarify that the one being used in is utils_3
@print_dev
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
       windowSpec = Window.partitionBy('ctr_store_var')

       ctr_store_sum = store_matching_df_var_tagged.select("*")

       for mech in mechanic_list:
         ctr_store_sum = ctr_store_sum.withColumn('sum_ctr_' + mech, F.sum(F.col('flag_ctr_' + mech)).over(windowSpec)).drop('flag_ctr_' + mech)

       # Select control stores level only and drop dupes
       ctr_store_sum_only = ctr_store_sum.drop('store_id', 'mech_name').drop_duplicates()

       ctr_store_mech_flag = ctr_store_sum_only.select("*")

       # Turn into Boolean columns
       for mech in mechanic_list:
         ctr_store_mech_flag = ctr_store_mech_flag.withColumn('ctr_' + mech, F.when(F.col('sum_ctr_' + mech) > 0, 1).otherwise(0)).drop('sum_ctr_' + mech)

       ctr_store_mech_flag = ctr_store_mech_flag.withColumnRenamed('ctr_store_var', 'store_id')

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
       txn_each_purchase = (all_feat_trans_trans_level.join(txn_test_store_media_aisles, on='household_id', how='left')
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
       txn_non_purchased = txn_all_test_control_adj.join(cmp_shppr_last_seen, on='household_id', how='leftanti')

       # For remaining Non-Purchased customers, group by and aggregate counts of how many times they have been exposed to each media
       # Only for transaction occuring in test stores during campaign period
       txn_non_purchased_test_dur = txn_non_purchased.filter(F.col('mech_name').isNotNull()).filter(F.col('date_id').between(F.col('c_start'), F.col('c_end')))
       nonpurchased_exposed_count = txn_non_purchased_test_dur.groupBy('household_id').pivot('mech_name').agg(F.countDistinct(F.col('transaction_uid'))).fillna(0)

       # For each mechanic, instead of count, change to flag (0 if no exposure, 1 if exposure regardless of count)
       mech_name_columns = nonpurchased_exposed_count.columns
       mech_name_columns.remove('household_id')

       nonpurchased_exposed_flagged = nonpurchased_exposed_count

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

   mechanic_list = test_store_sf.select('mech_name').drop_duplicates().rdd.flatMap(lambda x: x).collect()

   print("List of detected mechanics from store list: ", mechanic_list)

   if cust_uplift_lv == 'brand':
       prd_scope_df = brand_sf
   else:
       prd_scope_df = feat_sf

   ##---- Expose - UnExpose : Flag customer
   target_str = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
   #     cmp_exposed = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)

   ctr_str = _create_ctrl_store_sf(ctr_store_list=ctr_store_list, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
   #     cmp_unexposed = _get_exposed_cust(txn=txn, test_store_sf=ctr_str, adj_prod_sf=adj_prod_sf)

   filled_ctrl_store_sf_with_mech = _create_ctrl_store_sf_with_mech(filled_test_store_sf=target_str,
                                                                    filled_ctrl_store_sf=ctr_str,
                                                                    store_matching_df_var=store_matching_df_var,
                                                                    mechanic_list=mechanic_list)

   ## Tag exposed media of each shopper
   cmp_shppr_last_seen = _get_activ_mech_last_seen(txn=txn, test_store_sf=test_store_sf, ctr_str=ctr_str, adj_prod_sf=adj_prod_sf,
                                                   period_wk_col=period_wk_col, prd_scope_df=prd_scope_df,
                                                   cp_start_date=cp_start_date, cp_end_date=cp_end_date,
                                                   filled_ctrl_store_sf_with_mech=filled_ctrl_store_sf_with_mech)

   ## Find non-shoppers who are exposed and unexposed
   non_cmp_shppr_exposure = _get_non_shpprs_by_mech(txn=txn, adj_prod_sf=adj_prod_sf, cmp_shppr_last_seen=cmp_shppr_last_seen, test_store_sf=test_store_sf, ctr_str=ctr_str,
                                                    cp_start_date=cp_start_date, cp_end_date=cp_end_date, period_wk_col=period_wk_col,
                                                    filled_ctrl_store_sf_with_mech=filled_ctrl_store_sf_with_mech)

   ## Tag each customer by group for shopper group
   ## If no exposure flag in any mechanic, then Non-exposed Purchased
   ## If exposure in any mechanic, then Exposed Purchased
   num_of_mechanics = len(mechanic_list)

   cmp_shppr_last_seen_tag = cmp_shppr_last_seen.withColumn('total_mechanics_exposed',
                                                            sum(cmp_shppr_last_seen[col] for col in cmp_shppr_last_seen.columns[1:num_of_mechanics+1]))

   ## Tag each customer by group for non-shopper group
   ## If no exposure flag in any mechanic, then Non-exposed Non-purchased
   ## If exposure in any mechanic, then Exposed Non-purchased
   non_cmp_shppr_exposure_tag = non_cmp_shppr_exposure.withColumn('total_mechanics_exposed',
                                                                  sum(non_cmp_shppr_exposure[col] for col in non_cmp_shppr_exposure.columns[1:num_of_mechanics+1]))

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


   # Save and load temp table
   spark.sql('DROP TABLE IF EXISTS tdm_seg.cust_uplift_by_mech_temp')
   movement_and_exposure_by_mech.write.saveAsTable('tdm_seg.cust_uplift_by_mech_temp')

   movement_and_exposure_by_mech = spark.table('tdm_seg.cust_uplift_by_mech_temp')

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
                           .withColumn('cvs_rate_exposed_all_mech',
                                       F.col('Exposed_Purchased_all') / (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all'))) \
                           .withColumn('cvs_rate_unexposed_all_mech',
                                       F.col('Non_exposed_Purchased_all') / (F.col('Non_exposed_Purchased_all') + F.col('Non_exposed_Non_purchased_all'))) \
                           .withColumn('pct_uplift_all_mech',
                                       (F.col('cvs_rate_exposed_all_mech') / (F.col('cvs_rate_unexposed_all_mech'))) - 1) \
                           .withColumn('uplift_cust_all_mech',
                                       (F.col('cvs_rate_exposed_all_mech') - F.col('cvs_rate_unexposed_all_mech')) *
                                       (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all')))

   # Get only positive customer uplift for each customer group (New/Lapse/Existing)
   pstv_cstmr_uplift_all_mech_col = results.select('customer_group', 'uplift_cust_all_mech').filter("customer_group <> 'Total'") \
                                           .withColumn('pstv_cstmr_uplift_all_mech',
                                                       F.when(F.col('uplift_cust_all_mech') > 0, F.col('uplift_cust_all_mech')).otherwise(0))

   # Get Total customer uplift, ignoring negative values
   pstv_cstmr_uplift_all_mech_col = pstv_cstmr_uplift_all_mech_col.select('customer_group', 'pstv_cstmr_uplift_all_mech') \
                                                                  .unionByName(pstv_cstmr_uplift_all_mech_col.agg(F.sum(F.col('pstv_cstmr_uplift_all_mech')) \
                                                                                                                   .alias('pstv_cstmr_uplift_all_mech')).fillna(0) \
                                                                                                             .withColumn('customer_group', F.lit('Total')))

   results = results.join(pstv_cstmr_uplift_all_mech_col.select('customer_group', 'pstv_cstmr_uplift_all_mech'), on='customer_group', how='left')

   # Recalculate uplift using total positive customers
   results = results.withColumn('pct_positive_cust_uplift_all_mech',
                                (F.col('pstv_cstmr_uplift_all_mech') / (F.col('Exposed_Purchased_all') + F.col('Exposed_Non_purchased_all'))) / F.col('cvs_rate_unexposed_all_mech'))

   # Repeat for all mechanics if multiple mechanics
   if num_of_mechanics > 1:

     mech_result = {}
     pstv_cstmr_uplift_col = {}

     for mech in mechanic_list:
       mech_result[mech] = combine_n_cust.join(results.select('customer_group', 'cvs_rate_unexposed_all_mech'), on='customer_group', how='left') \
                                         .withColumn('cvs_rate_exposed_' + mech,
                                                     F.col('Exposed_Purchased_' + mech) /
                                                     (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech))) \
                                         .withColumn('cvs_rate_unexposed_' + mech,
                                                     F.col('Non_exposed_Purchased_' + mech) / (F.col('Non_exposed_Purchased_' + mech) + F.col('Non_exposed_Non_purchased_' + mech))) \
                                         .withColumn('pct_uplift_' + mech,
                                                     (F.col('cvs_rate_exposed_' + mech) / (F.col('cvs_rate_unexposed_' + mech))) - 1) \
                                         .withColumn('uplift_cust_' + mech,
                                                     (F.col('cvs_rate_exposed_' + mech) - F.col('cvs_rate_unexposed_' + mech)) *
                                                     (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech)))

       pstv_cstmr_uplift_col[mech] = mech_result[mech].select('customer_group', 'uplift_cust_' + mech).filter("customer_group <> 'Total'") \
                                                      .withColumn('pstv_cstmr_uplift_' + mech,
                                                                  F.when(F.col('uplift_cust_' + mech) > 0, F.col('uplift_cust_' + mech)).otherwise(0))

       pstv_cstmr_uplift_col[mech] = pstv_cstmr_uplift_col[mech].select('customer_group', 'pstv_cstmr_uplift_' + mech) \
                                                                .unionByName(pstv_cstmr_uplift_col[mech].agg(F.sum(F.col('pstv_cstmr_uplift_' + mech)) \
                                                                                                              .alias('pstv_cstmr_uplift_' + mech)).fillna(0) \
                                                                                                        .withColumn('customer_group', F.lit('Total')))

       mech_result[mech] = mech_result[mech].join(pstv_cstmr_uplift_col[mech].select('customer_group', 'pstv_cstmr_uplift_' + mech), on='customer_group', how='left')

       mech_result[mech] = mech_result[mech].withColumn('pct_positive_cust_uplift_' + mech,
                                                        (F.col('pstv_cstmr_uplift_' + mech) /
                                                         (F.col('Exposed_Purchased_' + mech) + F.col('Exposed_Non_purchased_' + mech))) /
                                                        F.col('cvs_rate_unexposed_' + mech))

       results = results.join(mech_result[mech].select('customer_group',
                                                       'cvs_rate_exposed_' + mech,
                                                       'cvs_rate_unexposed_' + mech,
                                                       'pct_uplift_' + mech,
                                                       'uplift_cust_' + mech,
                                                       'pstv_cstmr_uplift_' + mech,
                                                       'pct_positive_cust_uplift_' + mech),
                              on='customer_group', how='left')

   # Sort row order , export as SparkFrame
   df = results.toPandas()
   sort_dict = {"new":0, "existing":1, "lapse":2, "Total":3}
   df = df.sort_values(by=["customer_group"], key=lambda x: x.map(sort_dict))  # type: ignore
   uplift_out = spark.createDataFrame(df)

   return uplift_out, movement_and_exposure_by_mech

def get_cust_activated_prmzn(
        txn: SparkDataFrame,
        cp_start_date: str,
        cp_end_date: str,
        test_store_sf: SparkDataFrame,
        brand_sf: SparkDataFrame,
        feat_sf: SparkDataFrame,
        wk_type: str = "promozone"):
    """Media evaluation solution, Customer movement and switching v3 - PromoZon
    Activated shopper = feature product shopper at test store

    Parameters
    ----------
    txn:
        Snapped transaction of ppp + pre + cmp period
    cp_start_date
    cp_end_date
    wk_type:
    "fis_week" or "promo"
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
        """
        filled_test_store_sf = \
            (test_store_sf
            .fillna(str(cp_start_date), subset='c_start')
            .fillna(str(cp_end_date), subset='c_end')
            )
        return filled_test_store_sf

    def _get_exposed_cust(txn: SparkDataFrame,
                          test_store_sf: SparkDataFrame,
                          prd_scope_df: SparkDataFrame,
                          channel: str = "OFFLINE"
                          ) -> SparkDataFrame:
        """Get exposed customer & first exposed date
        """
        # Change filter column to offline_online_other_channel - Dec 2022 - Ta
        out = \
            (txn
             .where(F.col("offline_online_other_channel")==channel)
             .where(F.col("household_id").isNotNull())
             .join(test_store_sf, "store_id","inner") # Mapping cmp_start, cmp_end, mech_count by store
             .join(prd_scope_df, "upc_id", "inner")
             .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
             .groupBy("household_id")
             .agg(F.min("date_id").alias("first_exposed_date"))
            )
        return out

    def _get_shppr_prmzn(txn: SparkDataFrame,
                         period_wk_col_nm: str,
                         test_store_sf: SparkDataFrame,
                         prd_scope_df: SparkDataFrame,
                         channel: str = "OFFLINE",
                         ) -> SparkDataFrame:
        """Get first brand shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel
        """
        out = \
            (txn
             .where(F.col("channel")==channel)
             .where(F.col('household_id').isNotNull())
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(test_store_sf, "store_id", "inner")
             .join(prd_scope_df, 'upc_id')
             .groupBy('household_id')
             .agg(F.min('date_id').alias('first_shp_date'))
             .drop_duplicates()
            )
        return out

    def _get_activated_prmzn(exposed_cust: SparkDataFrame,
                             shppr_cust: SparkDataFrame
                             ) -> SparkDataFrame:
        """Get activated customer : Exposed in test store , shopped (brand/sku) in test store
        """
        out = \
            (exposed_cust.join(shppr_cust, "household_id", "inner")
             .select( shppr_cust.household_id.alias('cust_id')
                     ,shppr_cust.first_shp_date.alias('first_shp_date')
                    )
             .drop_duplicates()
            )
        return out
        
    def _get_activated_sales_prmzn(txn: SparkDataFrame
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
    
    ## End def
    
    #---- Main
    print("-"*80)
    print("Customer PromoZone Exposed & Activated")
    print("PromoZone Exposed & Activated = Shopped (Feature SKU/Feature Brand) in campaign period at test store")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    # Brand activate
    target_str = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
    cmp_exposed = _get_exposed_cust(txn=txn, test_store_sf=target_str, prd_scope_df=brand_sf)
    cmp_brand_shppr = _get_shppr_prmzn(txn=txn, test_store_sf=target_str, period_wk_col_nm=period_wk_col, prd_scope_df=brand_sf)
    cmp_brand_activated = _get_activated_prmzn(exposed_cust=cmp_exposed, shppr_cust=cmp_brand_shppr)

    #nmbr_brand_activated = cmp_brand_activated.count()
    #print(f'Total exposed and Feature Brand (in Category scope) shopper (Brand Activated) : {nmbr_brand_activated:,d}')
    brand_activated_info, brand_activated_sum =  _get_activated_sales_prmzn( txn=txn
                                                                            , shppr_actv   = cmp_brand_activated
                                                                            , prd_scope_df = brand_sf
                                                                            , prd_scope_nm = 'brand'
                                                                            , period_wk_col_nm = period_wk_col)

    print('\n Total exposed and Feature Brand (in Category scope) shopper (Brand Activated) Display below' )    
    brand_activated_sum.display()
    
    # Sku Activated
    cmp_sku_shppr = _get_shppr_prmzn(txn=txn, test_store_sf=target_str, period_wk_col_nm=period_wk_col, prd_scope_df=feat_sf)
    cmp_sku_activated = _get_activated_prmzn(exposed_cust=cmp_exposed, shppr_cust=cmp_sku_shppr)

    #nmbr_sku_activated = cmp_sku_activated.count()
    #print(f'Total exposed and Features SKU shopper (Features SKU Activated) : {nmbr_sku_activated:,d}')
    
    sku_activated_info, sku_activated_sum   =  _get_activated_sales_prmzn( txn=txn
                                                                          , shppr_actv   = cmp_sku_activated
                                                                          , prd_scope_df = feat_sf
                                                                          , prd_scope_nm = 'sku'
                                                                          , period_wk_col_nm = period_wk_col)

    print('\n Total exposed and Feature SKU shopper (SKU Activated) Display below' )
    
    sku_activated_sum.display()
    
    #return cmp_brand_activated, cmp_sku_activated
    return brand_activated_info, sku_activated_info, brand_activated_sum, sku_activated_sum

def get_cust_cltv(txn: SparkDataFrame,
                  cmp_id: str,
                  wk_type: str,
                  feat_sf: SparkDataFrame,
                  brand_sf: SparkDataFrame,
                  lv_svv_pcyc: str,
                  uplift_brand: SparkDataFrame,
                  media_spend: float,
                  svv_table: str,
                  pcyc_table: str,
                  cate_cd_list: List,
                  store_format: str
                  ):
    """(Uplift) Customer Life Time Value - EPOS @ Brand Level
    I) Calculate SpC metrics
        Use Brand activated customer, split customer into 2 groups
        A) 1-time brand buyer customer -> SpC of Feature Brand
        B) Muli-time brand buyer customer -> SpC of Feature Brand

    II) Use customer uplift to calculate CLTV-MyLo
        Use Customer Uplift @ Brand Level to calculate CLTV -> Extrapolate to EPOS

    III) Extrapolate CLTV-MyLo -> CLTV EPOS

    """
    import numpy as np

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

    def _get_brand_sec_id(feat_sf: SparkDataFrame):
        """Get brand_name, section_class_id, section_class_subclass_id
        """
        prd_feature = spark.table('tdm.v_prod_dim_c').where(F.col('division_id').isin([1,2,3,4,9,10,13])).join(feat_sf.select("upc_id").drop_duplicates(), "upc_id", "inner")
        sec_id_class_id_subclass_id_feature_product = prd_feature.select('section_id', 'class_id', 'subclass_id').drop_duplicates()
        sec_id_class_id_feature_product = prd_feature.select('section_id', 'class_id').drop_duplicates()
        brand_of_feature_product = prd_feature.select('brand_name').drop_duplicates()

        return sec_id_class_id_subclass_id_feature_product, sec_id_class_id_feature_product, brand_of_feature_product

    def _to_pandas(sf: SparkDataFrame # type: ignore
                   ) -> PandasDataFrame:
        """Solve DecimalType conversion to PandasDataFrame as string
        with pre-covert DecimalType to DoubleType then toPandas()
        automatically convert to float64

        Parameter
        ---------
        sf: pyspark.sql.DataFrame
            spark DataFrame to save

        Return
        ------
        :pandas.DataFrame
        """
        from pyspark.sql import types as T
        from pyspark.sql import functions as F

        col_name_type = sf.dtypes
        select_cast_exp = [F.col(c[0]).cast(T.DoubleType()) if c[1].startswith('decimal') else F.col(c[0]) for c in col_name_type]
        conv_sf = sf.select(*select_cast_exp)
        conv_df = conv_sf.toPandas()

        return conv_df  # type: ignore

    def _list2string(inlist, delim = ' '):
        """ This function use to convert from list variable to concatenate string
        having 2 input parameters
        inlist : list , that need to conver to string
        delim  : text , is a charactor use to be delimeter between each value in list
        """
        outtxt = ''
        n = 0

        for itm in inlist:

            if n == 0:
                outtxt = outtxt + str(itm)
                n      = n + 1
            else:
                outtxt = outtxt + str(delim) + str(itm)
                n      = n + 1
            ## end if
        ## end for

        return outtxt

    def _get_svv_df(svv_table: str,
                    lv_svv_pcyc: str,
                    feat_sf: SparkDataFrame,
                    cate_code_list: List
                    ):
        """Get survival rate and check if the feature sku is multi-brand / multi-subclass / multi-class
        Then use weighted average for survival rate
        """
        #---- Helper fn
        def __get_avg_cate_svv(svv_df, cate_lvl, cate_code_list):
            """Get category survival rate in case of NPD product
            Param
            -----
            svv_df : Spark dataframe of survival rate table
            cate_lvl : string : catevory level
            cate_code_list : list of category code that define brand category
            return cate_avg_survival_rate as SparkDataFrame

            ### add 3 more variable to use in case NPD and no survival rate at brand
            ## Pat 30 Jun 2022
            one_time_ratio
            spc_per_day
            AUC
            spc
            """
            if cate_lvl == 'subclass':
                svv  = svv_df.withColumn('subclass_code', F.concat(svv_df.division_id, F.lit('_'), svv_df.department_id, F.lit('_'), svv_df.section_id, F.lit('_'), svv_df.class_id, F.lit('_'), svv_df.subclass_id))
                ##cond = """subclass_code in ({}) """.format(cate_code_list)

                svv_cate = svv.where(svv.subclass_code.isin(cate_code_list))\
                            .select( svv.section_id.alias('sec_id')
                                    ,svv.class_id
                                    ,svv.subclass_code.alias('cate_code')
                                    ,svv.subclass_name.alias('cate_name')
                                    ,svv.brand_name
                                    ,svv.CSR_13_wks.alias('q1')
                                    ,svv.CSR_26_wks.alias('q2')
                                    ,svv.CSR_39_wks.alias('q3')
                                    ,svv.CSR_52_wks.alias('q4')
                                    ,svv.spending.alias('sales')
                                    ,svv.one_time_ratio.alias('otr')
                                    ,svv.AUC.alias('auc')
                                    ,svv.spc_per_day.alias('spd')
                                    )

            elif cate_lvl == 'class':
                svv  = svv_df.withColumn('class_code', F.concat(svv_df.division_id, F.lit('_'), svv_df.department_id, F.lit('_'), svv_df.section_id, F.lit('_'), svv_df.class_id))
                #cond = """class_code in ({}) """.format(cate_code_list)
                svv_cate = svv.where(svv.class_code.isin(cate_code_list))\
                            .select( svv.section_id.alias('sec_id')
                                    ,svv.class_id
                                    ,svv.class_code.alias('cate_code')
                                    ,svv.class_name.alias('cate_name')
                                    ,svv.brand_name
                                    ,svv.CSR_13_wks.alias('q1')
                                    ,svv.CSR_26_wks.alias('q2')
                                    ,svv.CSR_39_wks.alias('q3')
                                    ,svv.CSR_52_wks.alias('q4')
                                    ,svv.spending.alias('sales')
                                    ,svv.one_time_ratio.alias('otr')
                                    ,svv.AUC.alias('auc')
                                    ,svv.spc_per_day.alias('spd')
                                    )
            else:

                return None

            ## get weighted average svv by sale to category -- All sale values
            cate_sales  = svv_cate.agg(F.sum(F.col("sales"))).collect()[0][0]

            ## Pat add to get list of category name in case of multiple category define -- Pat 25 Jul 22

            cate_nm_lst = svv_cate.select(svv_cate.cate_name)\
                                .dropDuplicates()\
                                .toPandas()['cate_name'].to_list()

            cate_nm_txt = _list2string(cate_nm_lst, delim = ' , ')

            ## multiply for weighted
            svv_cate   = svv_cate.withColumn('pct_share_w', svv_cate.sales/cate_sales)\
                                .withColumn('w_q1', svv_cate.q1 * (svv_cate.sales/cate_sales))\
                                .withColumn('w_q2', svv_cate.q2 * (svv_cate.sales/cate_sales))\
                                .withColumn('w_q3', svv_cate.q3 * (svv_cate.sales/cate_sales))\
                                .withColumn('w_q4', svv_cate.q4 * (svv_cate.sales/cate_sales))\
                                .withColumn('w_otr',svv_cate.otr * (svv_cate.sales/cate_sales))\
                                .withColumn('w_auc',svv_cate.auc * (svv_cate.sales/cate_sales))\
                                .withColumn('w_spd',svv_cate.spd * (svv_cate.sales/cate_sales))

            svv_cate_wg_avg = svv_cate.agg( F.lit(cate_nm_txt).alias('category_name')
                                        ,F.lit(1).alias('CSR_0_wks')
                                        ,F.sum(svv_cate.w_q1).alias('CSR_13_wks_wavg')
                                        ,F.sum(svv_cate.w_q2).alias('CSR_26_wks_wavg')
                                        ,F.sum(svv_cate.w_q3).alias('CSR_39_wks_wavg')
                                        ,F.sum(svv_cate.w_q4).alias('CSR_52_wks_wavg')
                                        ,F.sum(svv_cate.w_otr).alias('one_time_ratio')
                                        ,F.sum(svv_cate.w_auc).alias('AUC')
                                        ,F.sum(svv_cate.w_spd).alias('spc_per_day')
                                        )

            return svv_cate_wg_avg

        def __get_avg_multi_brand_svv(brand_csr_sf: SparkDataFrame,
                                      cate_nm_txt: str,
                                      brand_nm_txt: str):
            """Get all KPIs weighted average of muli-brand survival rate
            Param
            -----
            brand_csr_sf: SparkDataFrame
                Initial retrived bradn csr
            cate_nm_txt: str
                Cleaned & conbined multi category name
            brand_nm_txt: str
                Cleaned & conbined multi brand name
            """
            brand_sales = brand_csr_sf.agg(F.sum(F.col("spending"))).collect()[0][0]  ## sum sales value of all brand

            brand_csr_w     = brand_csr_sf.withColumn('pct_share_w', brand_csr_sf.spending/brand_sales)\
                                        .withColumn('w_q1', brand_csr_sf.CSR_13_wks * (brand_csr_sf.spending/brand_sales))\
                                        .withColumn('w_q2', brand_csr_sf.CSR_26_wks * (brand_csr_sf.spending/brand_sales))\
                                        .withColumn('w_q3', brand_csr_sf.CSR_39_wks * (brand_csr_sf.spending/brand_sales))\
                                        .withColumn('w_q4', brand_csr_sf.CSR_52_wks * (brand_csr_sf.spending/brand_sales))\
                                        .withColumn('w_otr',brand_csr_sf.one_time_ratio * (brand_csr_sf.spending/brand_sales))\
                                        .withColumn('w_auc',brand_csr_sf.AUC            * (brand_csr_sf.spending/brand_sales))\
                                        .withColumn('w_spd',brand_csr_sf.spc_per_day    * (brand_csr_sf.spending/brand_sales))

            brand_csr_w_sf  = brand_csr_w.agg(F.lit(cate_nm_txt).alias('category_name')
                                            ,F.lit(brand_nm_txt).alias('brand_name')
                                            ,F.lit(1).alias('CSR_0_wks')
                                            ,F.sum(brand_csr_w.w_q1).alias('CSR_13_wks_wavg')
                                            ,F.sum(brand_csr_w.w_q2).alias('CSR_26_wks_wavg')
                                            ,F.sum(brand_csr_w.w_q3).alias('CSR_39_wks_wavg')
                                            ,F.sum(brand_csr_w.w_q4).alias('CSR_52_wks_wavg')
                                            ,F.sum(brand_csr_w.w_otr).alias('one_time_ratio')
                                            ,F.sum(brand_csr_w.w_auc).alias('AUC')
                                            ,F.sum(brand_csr_w.w_spd).alias('spc_per_day')
                                            ,F.avg(brand_csr_w.spending).alias('average_spending')
                                            ,F.lit(brand_sales).alias('all_brand_spending')
                                        )
            return brand_csr_w_sf

        #---- Main
        svv_tbl = spark.table(svv_table)

        sec_id_class_id_subclass_id_feature_product, sec_id_class_id_feature_product, brand_of_feature_product = _get_brand_sec_id(feat_sf=feat_sf)

        if lv_svv_pcyc.lower() == 'class':
            brand_csr_initial = svv_tbl.join(sec_id_class_id_feature_product, ['section_id', 'class_id']).join(brand_of_feature_product, ['brand_name'])

        elif lv_svv_pcyc.lower() == 'subclass':
            brand_csr_initial = svv_tbl.join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id']).join(brand_of_feature_product, ['brand_name'])

        brand_csr_initial_df = _to_pandas(brand_csr_initial)
        cate_nm_initial_lst = brand_csr_initial_df['class_name'].to_list()
        brand_nm_initial_lst = brand_csr_initial_df['class_name'].to_list()

        if len(brand_csr_initial_df) == 0: # use category average instead (call function 'get_avg_cate_svv')
            brand_csr = __get_avg_cate_svv(svv_tbl, lv_svv_pcyc, cate_cd_list)
            # brand_csr_df = brand_csr.toPandas()

            print('#'*80)
            brand_nm_txt = _list2string(brand_nm_initial_lst)
            print(' Warning !! Brand "', brand_nm_txt ,'" has no survival rate, Will use category average survival rate instead.')
            print('#'*80)

            print(' Display brand_csr use \n')
            brand_csr.display()

            use_cate_svv_flag = 1
            use_average_flag  = 0

        elif len(brand_csr_initial_df) > 1 :  # case has multiple brand in SKU, will need to do average
            brand_nm_txt = _list2string(cate_nm_initial_lst, ' and ')
            cate_nm_txt  = _list2string(brand_nm_initial_lst, ' and ')

            brand_nm_txt = str(brand_nm_txt)
            cate_nm_txt  = str(cate_nm_txt)

            brand_csr = __get_avg_multi_brand_svv(brand_csr_sf=brand_csr_initial, cate_nm_txt=cate_nm_txt, brand_nm_txt=brand_nm_txt)
            # brand_csr_df = brand_csr.toPandas()

            print('#'*80)
            print(' Warning !! Feature SKUs are in multiple brand name/multiple category, will need to do average between all data.')
            print(' Brand Name = ' + str(brand_nm_txt) )
            print(' Category code = ' + str(cate_nm_txt) )
            print('#'*80)

            print(' Display brand_csr use (before average) \n')
            brand_csr.display()

            use_cate_svv_flag = 0
            use_average_flag  = 1

        else:
            use_cate_svv_flag = 0
            use_average_flag  = 0

            print(' Display brand_csr use \n')
            brand_csr = brand_csr_initial
            brand_csr.display()

        return brand_csr, use_cate_svv_flag, use_average_flag

    def _get_shppr_kpi(txn: SparkDataFrame,
                       period_wk_col_nm: str,
                       prd_scope_df: SparkDataFrame,
                       activated_cust: SparkDataFrame,
                       ) -> SparkDataFrame:
        """Get first brand shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel
        """
        out = \
            (txn
             .where(F.col('household_id').isNotNull())
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(prd_scope_df, "upc_id", "inner")
             .join(activated_cust, "household_id", "inner")
             .groupBy('household_id')
             .agg(F.sum('net_spend_amt').alias('spending'),
                  F.countDistinct('transaction_uid').alias('visits'))
             )
        return out

    #---- Main
    print("-"*80)
    print("(Uplift) Customer Life Time Value (CLTV) - EPOS @ Brand level")
    print("Media Exposed = shopped in media aisle within campaign period (base on target input file) at target store , channel OFFLINE ")
    print("Brand Activated = Exposed & Shop Feature Brand in campaign period at any store format and any channel")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    #---- I) Calculate brand SpC of brand activated customer
    activated_cust = spark.table(f"tdm_seg.media_camp_eval_{cmp_id}_cust_brand_activated")
    brand_kpi = _get_shppr_kpi(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=brand_sf, activated_cust=activated_cust)
    spc_multi = brand_kpi.where(F.col('visits') > 1).agg(F.avg('spending').alias('spc_multi')).select('spc_multi').collect()[0][0]
    spc_onetime = brand_kpi.where(F.col('visits') == 1).agg(F.avg('spending').alias('spc_onetime')).select('spc_onetime').collect()[0][0]
    print(f"Spend per customers (multi): {spc_multi}")
    print(f"Spend per customers (one time): {spc_onetime}")

    #---- Load Survival rate based on lv_svv_pcyc (Switching Level)
    sec_id_class_id_subclass_id_feature_product, sec_id_class_id_feature_product, brand_of_feature_product = _get_brand_sec_id(feat_sf=feat_sf)
    lv_svv_pcyc = lv_svv_pcyc.lower()

    brand_csr, use_cate_svv_flag, use_average_flag = _get_svv_df(svv_table=svv_table, lv_svv_pcyc=lv_svv_pcyc, feat_sf=feat_sf, cate_code_list=cate_cd_list)
    brand_csr_df = _to_pandas(brand_csr)

    #---- Load Purchase cycle, based on lv_svv_pcyc (Switching Level)
   # where_cond   = """ store_format_group == {} """.format(store_format)
    
    if lv_svv_pcyc.lower() == 'class':
        pc_df    = spark.table(pcyc_table)\
                        .where(lower(F.col('store_format_group')) == store_format.lower())\
                        .join(sec_id_class_id_feature_product, ['section_id', 'class_id'])

    elif lv_svv_pcyc.lower() == 'subclass':
      
        pc_df    = spark.table(pcyc_table)\
                        .where(lower(F.col('store_format_group')) == store_format.lower())\
                        .join(sec_id_class_id_subclass_id_feature_product, ['section_id', 'class_id', 'subclass_id'])
   
    
    ## convert to pandas
    
    pc_table = _to_pandas(pc_df)
        
    #---- Customer Survival Rate graph
    brand_csr_graph = brand_csr_df[[c for c in brand_csr.columns if 'CSR' in c]].T
    brand_csr_graph.columns = ['survival_rate']
    brand_csr_graph.display()

    # ---- CLTV
    # Total uplift
    try:
        # support new customer uplift by mech
        total_uplift = uplift_brand.where(F.col("mechanic")=="all").where(F.col("customer_group")=="Total").select("pstv_cstmr_uplift").collect()[0][0]
    except:
        # Fall back to old customer uplift mech
        total_uplift = uplift_brand.where(F.col("customer_group")=="Total").select("pstv_cstmr_uplift").collect()[0][0]

    # Pre-calculated onetime
    one_time_ratio = brand_csr_df.one_time_ratio[0]
    one_time_ratio = float(one_time_ratio)

    auc = brand_csr_df.AUC[0]
    cc_pen = pc_table.cc_penetration[0]
    spc_per_day = brand_csr_df.spc_per_day[0]

    #---- calculate CLTV
    dur_cp_value = total_uplift* float(one_time_ratio) *float(spc_onetime) + total_uplift*(1-one_time_ratio)*float(spc_multi)
    post_cp_value = total_uplift* float(auc) * float(spc_per_day)
    cltv = dur_cp_value+post_cp_value
    epos_cltv = cltv/cc_pen
    print("EPOS CLTV: ", np.round(epos_cltv,2))

    #----- Break-even
    breakeven_df = brand_csr_graph.copy()
    # change first point to be one time ratio
    breakeven_df.loc['CSR_0_wks'] = one_time_ratio

    # calculate post_sales at each point of time in quarter value
    breakeven_df['post_sales'] = breakeven_df.survival_rate.astype(float) * total_uplift * float(spc_per_day) * 13*7  ## each quarter has 13 weeks and each week has 7 days

    breakeven_df.display()

    # calculate area under curve using 1/2 * (x+y) * a
    area = []
    for i in range(breakeven_df.shape[0]-1):
        area.append(0.5*(breakeven_df.iloc[i,1]+breakeven_df.iloc[i+1,1]))

    #create new data frame with value of sales after specific weeks
    breakeven_df2 = pd.DataFrame(area,index=[13,26,39,52],columns=['sales'])

    print("Display breakeven df area for starter : \n ")

    breakeven_df2.display()

    #set first day to be 0
    breakeven_df2.loc[0,'sales'] = 0
    breakeven_df2 = breakeven_df2.sort_index()

    #adding during campaign to every point in time
    breakeven_df2['sales+during'] = breakeven_df2+(dur_cp_value)

    #create accumulative columns
    breakeven_df2['acc_sales'] = breakeven_df2['sales+during'].cumsum()

    ## pat add
    print("Display breakeven table after add cumulative sales : \n ")
    breakeven_df2.display()

    #create epos value
    breakeven_df2['epos_acc_sales'] = breakeven_df2['acc_sales']/cc_pen.astype('float')
    breakeven_df2 = breakeven_df2.reset_index()
    breakeven_df2.round(2)

    #if use time more than 1 year, result show 1 year
    if media_spend > breakeven_df2.loc[4,'epos_acc_sales']:
        day_break = 'over one year'
    #else find point where it is breakeven
    else:
        index_break = breakeven_df2[breakeven_df2.epos_acc_sales>media_spend].index[0]

        # if it's in first period -> breakeven during campaing
        if index_break == 0:
            day_break = 0
            # else find lower bound and upper bound to calculate day left before break as a straight line
        else:
            low_bound = breakeven_df2.loc[index_break-1,'epos_acc_sales']
            up_bound = breakeven_df2.loc[index_break,'epos_acc_sales']
            low_bound_day =  breakeven_df2.loc[index_break-1,'index']*13*7
            day_break = low_bound_day+((media_spend - low_bound) *13*7 / (up_bound-low_bound))

    if type(day_break) == int:
        breakeven_time_month = day_break//30
        breakeven_time_day = day_break%30
        print(f"Breakeven time: {int(breakeven_time_month)} months {int(breakeven_time_day)} days")
    else:
        print(f"Breakeven time: More than a year")
        breakeven_time_month = 'More than a year'
        breakeven_time_day = 'More than a year'

    # ---- Calculate Customer Acquisition Cost (CAC) : 8 Jun 2022
    cac = 0 if total_uplift == 0 else media_spend/total_uplift #total_uplift_adj

    #create data frame for save
    df_cltv = pd.DataFrame({'measures':['Total Uplift Customers',
                                        'Total Uplift Customer (adj. negative)',
                                        'One time ratio',
                                        'Brand SpC onetime-buyer',
                                        'Brand SpC multi-buyer',
                                        'AUC',
                                        'Spend per Customer per Day',
                                        'CLTV',
                                        'CC Penetration',
                                        'Media Fee',
                                        'Customer Acquisition Cost (CAC)',
                                        'EPOS CLTV',
                                        'Breakeven Month',
                                        'Breakeven Day',
                                        'use_category_svv_flag',
                                        'use_average_cate_brand_flag'
                                       ],
                            'value':[total_uplift,
                                     total_uplift,
                                     one_time_ratio,
                                     spc_onetime,
                                     spc_multi,
                                     auc,
                                     spc_per_day,
                                     cltv,
                                     cc_pen,
                                     media_spend,
                                     cac,
                                     epos_cltv,
                                     breakeven_time_month,
                                     breakeven_time_day,
                                     use_cate_svv_flag,
                                     use_average_flag
                                   ]
                           })
    df_cltv.round(2)

    return df_cltv, brand_csr_graph

def _get_cust_brnd_swtchng_pntrtn(
        txn: SparkDataFrame,
        switching_lv: str,
        brand_df: SparkDataFrame,
        class_df: SparkDataFrame,
        sclass_df: SparkDataFrame,
        cust_movement_sf: SparkDataFrame,
        wk_type: str,
        ):
    """Dev version
    Customer brand switching and penetration
    Support feature brand in multi subclass
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper fn
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

    #---- Customer brand switching, re-write
    def _switching(cust_mv_brand_spend: SparkDataFrame,
               cust_mv_micro_flag: str,
               switching_lv:str,
               txn_feat_ctgry_pre: SparkDataFrame,
               agg_lvl: str,
               agg_lvl_re_nm: str ,
               period: str
               ):
        """
        switching_lv : class, sublass
        micro_flag : "new_to_brand"
        cust_mv_brand_spend : SparkDataFrame of hh_id, customer_macro_flag, *customer_micro_flag, all hierachy, brand spend for pre - dur
        txn_feat_ctgry_pre : all txn in prior+pre period of feature subclass/class based on switching_lv
        grp : fix ['class_name', 'brand_name'] ; for group by pri+pre sales, n_cust
        prod_lev: fix 'brand' ; for column name of kpi => oth_{}_spend , => oth_{}_custs
        full_prod_lev: fix 'brand_in_category' ;
        col_rename: fix 'brand_name' ; rename in param `grp` ['class_name', 'brand_name'] =>  ['class_name', 'oth_brand_in_category'']
        period: 'dur'
        """
        print("-"*80)
        print(f"Customer movement micro level group : '{cust_mv_micro_flag}'")
        print(f"Switching of '{agg_lvl}' in '{switching_lv}'")

        # filter hh_id, brand spend pre, dur of desired movement micro_flag, defaut 'new_to_brand'
        cust_mv_micro_brand_spend = cust_mv_brand_spend.where(F.col('customer_micro_flag') == cust_mv_micro_flag)

        # txn in Pri+Pre of features subclass/class, for cust mv micro gr
        txn_cust_mv_micro_pre = \
            (txn_feat_ctgry_pre
             .join(cust_mv_micro_brand_spend.select('household_id').dropDuplicates(), on='household_id', how='inner')
            )
        # Agg sales, n_cust of `higher 1 lvl`, brand name for cust micro mv group in pre period
        # if switching = `class` -> Agg at section (to support multi class switching) and so on
        # change column "brand_name" -> oth_brand_in_category

        if switching_lv == "class":
            higher_lvl = "section_name"
            higher_grpby_lvl = ["section_name", agg_lvl]
            dur_brand_spend_grpby = ['division_name','department_name','section_name',
                                     F.col("brand_name").alias("original_brand"),
                                     'customer_macro_flag','customer_micro_flag']
        elif switching_lv == "subclass":
            higher_lvl = "class_name"
            higher_grpby_lvl = ["class_name", agg_lvl]
            dur_brand_spend_grpby = ['division_name','department_name','section_name',
                                     "class_name",
                                     F.col("brand_name").alias("original_brand"),
                                     'customer_macro_flag','customer_micro_flag']
        else:
            print("Use default class switching result")
            higher_lvl = "class_name"
            higher_grpby_lvl = ["class_name", agg_lvl]
            dur_brand_spend_grpby = ['division_name','department_name','section_name',
                                     F.col("brand_name").alias("original_brand"),
                                     'customer_macro_flag','customer_micro_flag']

        print(f"To support multi-{switching_lv} switching, Aggregate of brand 1-higher lv at : ", higher_grpby_lvl)

        cust_mv_higher_lvl_kpi_pre = \
        (txn_cust_mv_micro_pre
         .groupby(higher_grpby_lvl)
         .agg(F.sum('net_spend_amt').alias(f'oth_{agg_lvl}_spend'),
              F.countDistinct('household_id').alias(f'oth_{agg_lvl}_customers'))
         .withColumnRenamed(agg_lvl, f'oth_{agg_lvl_re_nm}')
         .withColumn(f"total_oth_{agg_lvl}_spend", F.sum(f'oth_{agg_lvl}_spend').over(Window.partitionBy()))
        )

        # Agg during period, brand spend
        cust_mv_micro_dur_brand_kpi = \
        (cust_mv_micro_brand_spend
            .groupby(dur_brand_spend_grpby)
            .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                F.countDistinct('household_id').alias('total_ori_brand_cust'))
        )
        cust_mv_kpi_pre_dur = cust_mv_micro_dur_brand_kpi.join(cust_mv_higher_lvl_kpi_pre,
                                                                on=higher_lvl, how='inner')
        # Select column, order by pct_cust_oth
        if switching_lv == "class":
            sel_col = ['division_name','department_name','section_name',
                       "original_brand", 'customer_macro_flag','customer_micro_flag',
                       'total_ori_brand_cust','total_ori_brand_spend',
                       'oth_'+agg_lvl_re_nm,'oth_'+agg_lvl+'_customers',
                       'oth_'+agg_lvl+'_spend','total_oth_'+agg_lvl+'_spend']
        elif switching_lv == "subclass":
            sel_col = ['division_name','department_name','section_name',
                       "class_name",
                       "original_brand", 'customer_macro_flag','customer_micro_flag',
                       'total_ori_brand_cust','total_ori_brand_spend',
                       'oth_'+agg_lvl_re_nm,'oth_'+agg_lvl+'_customers',
                       'oth_'+agg_lvl+'_spend','total_oth_'+agg_lvl+'_spend']
        else:
            sel_col = ['division_name','department_name','section_name',
                       "original_brand", 'customer_macro_flag','customer_micro_flag',
                       'total_ori_brand_cust','total_ori_brand_spend',
                       'oth_'+agg_lvl_re_nm,'oth_'+agg_lvl+'_customers',
                       'oth_'+agg_lvl+'_spend','total_oth_'+agg_lvl+'_spend']

        switching_result = \
        (cust_mv_kpi_pre_dur
         .select(sel_col)
         .withColumn('pct_cust_oth_'+agg_lvl_re_nm, F.col('oth_'+agg_lvl+'_customers')/F.col('total_ori_brand_cust'))
         .withColumn('pct_spend_oth_'+agg_lvl_re_nm, F.col('oth_'+agg_lvl+'_spend')/F.col('total_oth_'+agg_lvl+'_spend'))
         .checkpoint()
        )

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
                      # "class_name",  # TO BE DONE support for multi subclass
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
        new_to_brand_switching_from = _switching(cust_movement_pre_dur_spend,
                                                 'new_to_brand',
                                                 switching_lv,
                                                 prior_pre_cc_txn_prd_scope,
                                                 "brand_name",
                                                 "brand_in_category",
                                                 'dur')
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

def main():
    pass

if __name__ == "__main__":
    main()
