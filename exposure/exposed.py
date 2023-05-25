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

def create_txn_offline_x_aisle_target_store(cmp: CampaignEval):
    
    cmp.txn_offline_x_aisle_target_store = \
        (cmp.txn.join(cmp.aisle_target_store_conf, ["store_id", "upc_id", "date_id"])
         .where(F.col("offline_online_other_channel")=="OFFLINE")
        )
    cmp.txn_offline_x_aisle_target_store = cmp.txn_offline_x_aisle_target_store.checkpoint()
    pass

def get_store_mech_exposure_cmp(cmp: CampaignEval):
    
    create_txn_offline_x_aisle_target_store(cmp)
    
    STORE_FMT_FAMILY_SIZE = cmp.spark.createDataFrame([("hde", 2.2), ("talad", 1.5), ("gofresh", 1.0)],["store_format_name", "family_size"])
    family_size = STORE_FMT_FAMILY_SIZE.where(F.col("store_format_name")==cmp.store_fmt.lower())

    str_mech_visits = \
        (cmp.txn_offline_x_aisle_target_store
            .groupBy("store_id", "store_region", "mech_name", "store_format_name")
            .agg(F.avg(F.col("mech_count")).alias("mech_count"),
                F.avg(F.col("media_fee_psto")).alias("media_fee"),
                F.count_distinct('transaction_uid').alias('epos_visits'),
                F.count_distinct((F.when(F.col('customer_id').isNotNull(), F.col(
                'transaction_uid')).otherwise(None))).alias('carded_visits'),
                F.count_distinct((F.when(F.col('customer_id').isNull(), F.col(
                'transaction_uid')).otherwise(None))).alias('non_carded_visits')
                )
        )
            
    cmp.str_mech_exposure_cmp = \
        (str_mech_visits
            .join(STORE_FMT_FAMILY_SIZE, "store_format_name", "left")
            .withColumn('epos_impression', F.col('epos_visits')*F.col("family_size")*F.col('mech_count'))
            .withColumn('carded_impression', F.col('carded_visits')*F.col("family_size")*F.col('mech_count'))
            .withColumn('non_carded_impression', F.col('non_carded_visits')*F.col("family_size")*F.col('mech_count'))
            .withColumn("cpm", F.col("media_fee")/ (F.col('epos_visits')*F.col("family_size")*F.col('mech_count')/1000) )
        )
    pass

def _exposure_all(cmp: CampaignEval):
            
    #---- Overall Exposure    
    all_impression = \
    (cmp.str_mech_exposure_cmp
        .agg(F.sum('epos_visits').alias('epos_visits'),
            F.sum('carded_visits').alias('carded_visits'),
            F.sum('non_carded_visits').alias('non_carded_visits'),
            F.sum('epos_impression').alias('epos_impression'),
            F.sum('carded_impression').alias('carded_impression'),
            F.sum('non_carded_impression').alias('non_carded_impression'),
            F.sum('media_fee').alias("media_fee"),
            (F.sum("media_fee") / ( F.sum('epos_impression') / 1000)).alias("cpm")
            )
        )

    all_store_customer = cmp.txn_x_aisle_target_store.agg(F.count_distinct(F.col('household_id')).alias('carded_customers')).collect()[0][0]
    
    exposure_all = \
    (all_impression
        .withColumn('carded_reach', F.lit(all_store_customer))
        .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach'))
        .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq'))
        .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
        .withColumn('media_spend', F.col("media_fee"))
        .withColumn('CPM', F.col("cpm"))
        )
        
    return exposure_all

def _exposure_region(cmp: CampaignEval):
    
    customer_by_region = cmp.txn_x_aisle_target_store.groupBy('store_region').agg(
        F.countDistinct(F.col('household_id')).alias('carded_customers'))
    
    region_impression = \
        (cmp.str_mech_exposure_cmp
         .groupBy('store_region')
         .agg(F.sum('epos_visits').alias('epos_visits'),
              F.sum('carded_visits').alias('carded_visits'),
              F.sum('non_carded_visits').alias('non_carded_visits'),
              F.sum('epos_impression').alias('epos_impression'),
              F.sum('carded_impression').alias('carded_impression'),
              F.sum('non_carded_impression').alias('non_carded_impression'),
              F.sum('media_fee').alias("media_fee"),
              (F.sum("media_fee") / ( F.sum('epos_impression') / 1000)).alias("cpm"),
              )
         .join(customer_by_region, "store_region", "left")
         .withColumn('carded_reach', F.col('carded_customers'))
         .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach'))
         .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq'))
         .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
        )
        
    return region_impression
    
def get_exposure(cmp: CampaignEval):
        
    if cmp.params["aisle_mode"] in ["total_store"]:
        cmp.params["exposure_type"] = "store_lv"
        create_txn_offline_x_aisle_target_store(cmp)
        exposure_all = _exposure_all(cmp)
        exposure_region = _exposure_region(cmp)
        return exposure_all, exposure_region

    elif cmp.params["aisle_mode"] in ["homeshelf", "cross_cate"]:
        cmp.params["exposure_type"] = "aisle_lv"
        create_txn_offline_x_aisle_target_store(cmp)
        exposure_all = _exposure_all(cmp)
        exposure_region = _exposure_region(cmp)
        return exposure_all, exposure_region

    elif cmp.params["aisle_mode"] in ["target_store_config"]:
        cmp.params["exposure_type"] = "target_store_config"
        create_txn_offline_x_aisle_target_store(cmp)
        exposure_all = _exposure_all(cmp)
        exposure_region = _exposure_region(cmp)
        return exposure_all, exposure_region
        
def get_awareness(cmp: CampaignEval):
    """For Awareness of HDE, Talad

    """
    # get only txn in exposure area in test store
    print('='*80)
    print('Exposure v.3 - add media mechanics multiplyer by store & period by store')
    print('Exposed customer (test store) from "OFFLINE" channel only')

    print('Check test store input column')
    cmp.target_store.display()

    # Family size for HDE, Talad

    if cmp.store_fmt in ["hde", "hyper"]:
        family_size = 2.2
    elif cmp.store_fmt in ["talad", "super"]:
        family_size = 1.5
    elif cmp.store_fmt in ["gofresh", "mini_super"]:
        family_size = 1.0
    else:
        family_size = 1.0

    print(
        f'For store format "{cmp.store_fmt}" family size : {family_size:.2f}')
    cmp.params["family_size"] = family_size

    # Join txn with test store details & adjacency product
    # If not defined c_start, c_end will use cp_start_date, cp_end_date
    # Filter period in each store by period defined in test store
    # Change filter column to offline_online_other_channel - Dec 2022 - Ta
    txn_exposed = \
        (cmp.txn
         .filter(F.col('offline_online_other_channel') == 'OFFLINE')
         # Mapping cmp_start, cmp_end, mech_count by store
         .join(cmp.target_store, 'store_id', 'inner')
         .join(cmp.aisle_sku, 'upc_id', 'inner')
         .fillna(str(cmp.cmp_start), subset='c_start')
         .fillna(str(cmp.cmp_end), subset='c_end')
         # Filter only period in each mechanics
         .filter(F.col('date_id').between(F.col('c_start'), F.col('c_end')))
         )

    # ---- Overall Exposure
    by_store_impression = \
        (txn_exposed
         .groupBy('store_id', 'mech_count')
         .agg(
             F.countDistinct('transaction_uid').alias('epos_visits'),
             F.countDistinct((F.when(F.col('customer_id').isNotNull(), F.col(
                 'transaction_uid')).otherwise(None))).alias('carded_visits'),
             F.countDistinct((F.when(F.col('customer_id').isNull(), F.col(
                 'transaction_uid')).otherwise(None))).alias('non_carded_visits')
         )
            .withColumn('epos_impression', F.col('epos_visits')*family_size*F.col('mech_count'))
            .withColumn('carded_impression', F.col('carded_visits')*family_size*F.col('mech_count'))
            .withColumn('non_carded_impression', F.col('non_carded_visits')*family_size*F.col('mech_count'))
         )

    all_impression = \
        (by_store_impression
         .agg(F.sum('epos_visits').alias('epos_visits'),
              F.sum('carded_visits').alias('carded_visits'),
              F.sum('non_carded_visits').alias('non_carded_visits'),
              F.sum('epos_impression').alias('epos_impression'),
              F.sum('carded_impression').alias('carded_impression'),
              F.sum('non_carded_impression').alias('non_carded_impression')
              )
         )

    all_store_customer = txn_exposed.agg(F.countDistinct(
        F.col('household_id')).alias('carded_customers')).collect()[0][0]

    exposure_all = \
        (all_impression
         .withColumn('carded_reach', F.lit(all_store_customer))
         .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach'))
         .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq'))
         .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
         .withColumn('media_spend', F.lit(cmp.media_fee))
         .withColumn('CPM', F.lit(cmp.media_fee) / (F.col('epos_impression') / 1000))
         )

    # ---- By Region Exposure

    by_store_region_impression = \
        (txn_exposed
         .groupBy('store_id', 'store_region', 'mech_count')
         .agg(
             F.countDistinct('transaction_uid').alias('epos_visits'),
             F.countDistinct((F.when(F.col('customer_id').isNotNull(), F.col(
                 'transaction_uid')).otherwise(None))).alias('carded_visits'),
             F.countDistinct((F.when(F.col('customer_id').isNull(), F.col(
                 'transaction_uid')).otherwise(None))).alias('non_carded_visits')
         )
            .withColumn('epos_impression', F.col('epos_visits')*family_size*F.col('mech_count'))
            .withColumn('carded_impression', F.col('carded_visits')*family_size*F.col('mech_count'))
            .withColumn('non_carded_impression', F.col('non_carded_visits')*family_size*F.col('mech_count'))
         )

    region_impression = \
        (by_store_region_impression
         .groupBy('store_region')
         .agg(F.sum('epos_visits').alias('epos_visits'),
              F.sum('carded_visits').alias('carded_visits'),
              F.sum('non_carded_visits').alias('non_carded_visits'),
              F.sum('epos_impression').alias('epos_impression'),
              F.sum('carded_impression').alias('carded_impression'),
              F.sum('non_carded_impression').alias('non_carded_impression')
              )
         )

    customer_by_region = txn_exposed.groupBy('store_region').agg(
        F.countDistinct(F.col('household_id')).alias('carded_customers'))

    # Allocate media by region
    count_test_store_all = cmp.target_store.select(
        'store_id').drop_duplicates().count()

    # combine region for gofresh
    count_test_store_region = \
        (cmp.store_dim
         .join(cmp.target_store.select('store_id').drop_duplicates(), 'store_id', 'inner')
         .groupBy('store_region')
         .agg(F.count('store_id').alias('num_test_store'))
         )

    media_by_region = \
        (count_test_store_region
         .withColumn('num_all_test_stores', F.lit(count_test_store_all))
         .withColumn('all_media_spend', F.lit(cmp.media_fee))
         .withColumn('region_media_spend', F.col('all_media_spend')/F.col('num_all_test_stores')*F.col('num_test_store'))
         )

    exposure_region = \
        (region_impression
         .join(customer_by_region, 'store_region', 'left')
         .join(media_by_region, 'store_region', 'left')
         .withColumn('carded_reach', F.col('carded_customers'))
         .withColumn('avg_carded_freq', F.col('carded_visits')/F.col('carded_reach'))
         .withColumn('est_non_carded_reach', F.col('non_carded_visits')/F.col('avg_carded_freq'))
         .withColumn('total_reach', F.col('carded_reach') + F.col('est_non_carded_reach'))
         .withColumn('CPM', F.col('region_media_spend') / (F.col('epos_impression') / 1000))
         )

    return exposure_all, exposure_region
