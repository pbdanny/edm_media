import pprint
from ast import literal_eval
from typing import List, Union
from datetime import datetime, timedelta
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval, CampaignEvalO3

from utils import period_cal
from activate import activated
from matching import store_matching
from utils import helper

#---- Create txn offline at aisle of matched store
def create_txn_offline_x_aisle_matched_store(cmp):
    """Create offline transaction data with aisle information based on matched stores.

    This function creates offline transaction data with aisle information based on matched stores for the provided CampaignEval object (`cmp`). It performs the following steps:

    1. Checks if the `txn_offline_x_aisle_matched_store` attribute already exists in the `cmp` object. If it does, the function returns immediately.
    2. Calls the `get_store_matching_across_region` function from the `store_matching` module to perform store matching across regions.
    3. Matches the aisle information to the target store configuration based on the matched stores. It joins the `aisle_target_store_conf` DataFrame with the `matched_store` DataFrame using the `store_id` and `ctrl_store_id` columns, and renames the `ctrl_store_id` column to `store_id`.
    4. Creates the `txn_offline_x_aisle_matched_store` DataFrame by joining the transaction data (`txn`) with the aisle-matched store information. It performs the join on the `store_id`, `upc_id`, and `date_id` columns and filters the data to include only offline channel transactions.
    5. Updates the `cmp` object by assigning the created aisle-matched store DataFrame to the `aisle_matched_store` attribute.
    6. Returns None.

    Args:
        cmp (CampaignEval): The CampaignEval object containing the necessary data for creating the offline transaction data with aisle information.

    Returns:
        None: This function does not return a value explicitly.
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
         .where(F.col("channel_flag")=="OFFLINE")
        )
        
    return

#---- UnExposure any mechnaics 
def get_cust_first_unexposed_any_mech(cmp):
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

def get_cust_any_mech_unexposed_purchased(cmp,
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
@helper.timer
def get_cust_txn_all_unexposed_date_n_mech(cmp):
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
@helper.timer
def get_cust_by_mech_unexposed_purchased(cmp,
                                         prd_scope_df: SparkDataFrame,
                                         prd_scope_nm: str):
    """Get the count of customers who made purchases of specific products within a given product scope after being exposed to specific mechanics.

    This function calculates the count of customers who made purchases of specific products within a given product scope after being exposed to specific mechanics. It takes the following inputs:

    Args:
        cmp (CampaignEval): The CampaignEval object containing the necessary data for customer evaluation.
        prd_scope_df (SparkDataFrame): The Spark DataFrame containing the product scope data.
        prd_scope_nm (str): The name of the product scope.

    Returns:
        SparkDataFrame: A Spark DataFrame containing the count of customers who made purchases of specific products within the product scope after being exposed to specific mechanics. The DataFrame has the following columns:
            - household_id: The unique identifier of the household.
            - mech_name: The name of the mechanic.
            - n_visit_purchased_unexposure: The count of visits where the customer made a purchase within the product scope after being exposed to the mechanic.
    """    
    cust_all_unexposed = get_cust_txn_all_unexposed_date_n_mech(cmp)
    cust_all_prod_purchase = activated.get_cust_txn_all_prod_purchase_date(cmp, prd_scope_df)

    txn_each_purchase_most_recent_media_unexposed = \
    (cust_all_unexposed
    .join(cust_all_prod_purchase, on='household_id', how='inner')
    .where(F.col('unexposed_tran_datetime') <= F.col('purchase_tran_datetime'))
    .withColumn('time_diff', F.col('purchase_tran_datetime') - F.col('unexposed_tran_datetime'))
    .withColumn('recency_rank', F.dense_rank().over(Window.partitionBy('household_id','purchase_transaction_uid').orderBy(F.col('time_diff'))))
    .where(F.col('recency_rank') == 1)
    .select('household_id', 'purchase_transaction_uid', 'mech_name')
    .drop_duplicates()
    )
    cust_purchased_unexposure_count = \
        (txn_each_purchase_most_recent_media_unexposed
        .groupBy('household_id', 'mech_name')
        .agg(F.count_distinct(F.col('purchase_transaction_uid')).alias("n_visit_purchased_unexposure"))
        )
    return cust_purchased_unexposure_count

def get_cust_by_mech_last_seen_unexposed_tag(cmp,
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
