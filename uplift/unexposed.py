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
from matching import store_matching

#---- Create txn offline at aisle of matched store
def create_txn_offline_x_aisle_matched_store(cmp: CampaignEval):
    """Create txn offline x aisle based on matched store
    UnExposed
    - Offline channel
    - Matched store
    - Aisle definition based on matched target store aisle
    - Period based on matched target store config
    """
    if hasattr(cmp, "txn_offline_x_aisle_matched_store"):
        return
    store_matching.get_store_matching_across_region(cmp)
    
    cmp.aisle_matched_store = \
    (cmp.aisle_target_store_conf
     .join(cmp.matched_store.select(F.col("test_store_id").alias("store_id"), "ctrl_store_id").drop_duplicates(),
           "store_id"
           )
     .drop("store_id")
     .withColumnRenamed("ctrl_store_id", "store_id")
    )
    
    cmp.txn_offline_x_aisle_matched_store = \
        (cmp.txn.join(cmp.aisle_matched_store, ["store_id", "upc_id", "date_id"])
         .where(F.col("offline_online_other_channel")=="OFFLINE")
        )
        
    return

#---- UnExposure any mechnaics 
def get_cust_first_unexposed_any_mech(cmp: CampaignEval):
    """First unexposure any mechanics, ignore difference of mechanic name, 
    - UnExposure
        - Period based on target store config
        - Matched ctrl store
        - Offline channel
        - First unexposure, any mechanics
    """
    create_txn_offline_x_aisle_matched_store(cmp)
        
    cust_first_unexposed = \
        (cmp.txn_offline_x_aisle_matched_store
         .where(F.col("household_id").isNotNull())
         .groupBy("household_id")
         .agg(F.min("date_id").alias("first_unexposed_date"))
        )
    return cust_first_unexposed

def get_cust_any_mech_unexposed_purchased(cmp: CampaignEval,
                                          prd_scope_df: SparkDataFrame,
                                          prd_scope_nm: str):
    """Get product scope (feature sku / feature brand) unexposed - purchased
    - UnExposure
        - Period based on target store config
        - Matched ctrl store
        - Offline channel
        - First unexposure, any mechanics
            
    - First product purchase date
        - within "DUR" period
        - Any store
        - Any channel
    """
    cust_first_unexposed = get_cust_first_unexposed_any_mech(cmp)
    cust_first_prod_purchase = activated.get_cust_first_prod_purchase_date(cmp, prd_scope_df)
    
    cust_unexposed_purchased = \
        (cust_first_unexposed
         .join(cust_first_prod_purchase, "household_id", "left")
             .where(F.col('first_unexposed_date').isNotNull())
             .where(F.col('first_purchase_date').isNotNull())
             .where(F.col('first_unexposed_date') <= F.col('first_purchase_date'))
             .select(F.col("household_id"),
                     F.col('first_unexposed_date'),
                     F.col("first_purchase_date")
                    )
             .drop_duplicates()
             .withColumn("customer_group", F.lit("unexposed_purchased"))
             .withColumn('level', F.lit(prd_scope_nm))

             )
    return cust_unexposed_purchased

#---- Unexposure by mechanics
def get_cust_txn_all_unexposed_date_n_mech(cmp: CampaignEval):
    """household_id unexposed by mech_name
    - Improve version : add aisle_scope
    """
    create_txn_offline_x_aisle_matched_store(cmp)
    cust_txn_unexposed_mech = \
        (cmp.txn_offline_x_aisle_matched_store
         .where(F.col("household_id").isNotNull())
         .select('household_id', 'transaction_uid', 'tran_datetime', 'mech_name')
         .drop_duplicates()
         .withColumnRenamed('transaction_uid', 'unexposed_transaction_uid')
         .withColumnRenamed('tran_datetime', 'unexposed_tran_datetime')
        )
    return cust_txn_unexposed_mech

def get_cust_by_mech_unexposed_purchased(cmp: CampaignEval,
                                         prd_scope_df: SparkDataFrame,
                                         prd_scope_nm: str):
    cust_all_unexposed = get_cust_txn_all_unexposed_date_n_mech(cmp)
    cust_all_prod_purchase = activated.get_cust_txn_all_prod_purchase_date(cmp, prd_scope_df)

    txn_each_purchase_most_recent_media_unexposed = \
    (cust_all_unexposed
    .join(cust_all_prod_purchase, on='household_id', how='inner')
    .where(F.col('unexposed_tran_datetime') <= F.col('purchase_tran_datetime'))
    .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('unexposed_tran_datetime'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('purchase_transaction_uid').orderBy(F.col('time_diff'))))
    .where(F.col('recency_rank') == 1).drop_duplicates()
    )
    cust_purchased_unexposure_count = \
        (txn_each_purchase_most_recent_media_unexposed
        .groupBy('household_id', 'mech_name')
        .agg(F.count_distinct(F.col('purchase_transaction_uid')).alias("n_visit_purchased_unexposure"))
        )
    return cust_purchased_unexposure_count

def get_cust_by_mech_last_seen_unexposed_tag(cmp: CampaignEval,
                                             prd_scope_df: SparkDataFrame,
                                             prd_scope_nm: str):
    
    purchased_unexposure_count = get_cust_by_mech_unexposed_purchased(cmp, prd_scope_df, prd_scope_nm)
    
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
