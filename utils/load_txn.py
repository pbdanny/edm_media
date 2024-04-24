import pprint
from ast import literal_eval
from typing import List, Union
from datetime import datetime, timedelta
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

import pandas as pd
import numpy as np
from pathlib import Path

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval, CampaignEvalO3
from utils import period_cal

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
from edm_class import txnItem

def load_txn(cmp: Union[CampaignEval, CampaignEvalO3],
             txn_mode: str = "central_trans_media"):
    """Load transaction

    Parameters
    ----------
    txn_mode: str, default = "central_media_trans"
        "central_trans_media" : load from pregenerated tdm_dev.v_th_central_transaction_item_media
        "stored_campaign_txn" : load from created tdm_dev.media_campaign_eval_txn_data_{cmp.params['cmp_id']}
        "create_new" : create from raw table
    """
    if txn_mode == "create_new":
        cmp.params["txn_mode"] == "create_new"
        snap = txnItem(end_wk_id=cmp.cmp_en_wk,
                       str_wk_id=cmp.ppp_st_wk,
                       manuf_name=False,
                       head_col_select=["transaction_uid", "date_id",
                                        "store_id", "channel", "pos_type", "pos_id"],
                       item_col_select=['transaction_uid', 'store_id', 'date_id', 'upc_id', 'week_id',
                                        'net_spend_amt', 'unit', 'customer_id', 'discount_amt', 'cc_flag'],
                       prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name',
                                        'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
                                        'department_code', 'section_code', 'class_code', 'subclass_code'])

        cmp.txn = snap.txn
        print("Created txn from module edm_class.txnItem")

    elif txn_mode == "stored_campaign_txn":
        try:
            cmp.txn = cmp.spark.table(
                f"tdm_dev.media_campaign_eval_txn_data_{cmp.params['cmp_id'].lower()}")
            cmp.params["txn_mode"] = "stored_campaign_txn"
            print("Load txn from previous evaluation ran")
        except Exception as e:
            cmp.params["txn_mode"] = "central_trans_media"
            cmp.txn = cmp.spark.table("tdm_dev.v_th_central_transaction_item_media")
            print("Could not load txn from previous evaluation ran")
            print("Fallback to load txn from from v_th_central_transaction_item_media")
    else:
        cmp.params["txn_mode"] = "central_trans_media"
        cmp.txn = cmp.spark.table("tdm_dev.v_th_central_transaction_item_media")
        print("Load txn from v_th_central_transaction_item_media")

    create_period_col(cmp)
    scope_txn(cmp)
    replace_brand_nm(cmp)
    replace_custom_upc_details(cmp)
    replace_store_region(cmp)
    forward_compatible_stored_txn_schema(cmp)

    return

def replace_brand_nm(cmp: Union[CampaignEval, CampaignEvalO3]):
    """Replace the transactions of multi-feature brands with the first main brand.

    This function replaces the brand names in the transactions with the brand names from the main products. 
    It ensures that each transaction is associated with a single brand, even if the product has multiple features.

    Args:
        cmp (CampaignEval): The CampaignEval object containing the transactions and product information.

    Returns:
        None
    """
    cmp.txn = \
    (cmp.txn
        .drop("brand_name")
        .join(cmp.product_dim.select("upc_id", "brand_name"), 'upc_id', 'left')
        .fillna('Unidentified', subset='brand_name')
    )

    return

def replace_custom_upc_details(cmp):
    """Replace the transactions with custom upc details

    This function replaces all the product dim in transactions with the brand names from the main products. 
    It ensures that each transaction is associated with a single brand, even if the product has multiple features.

    Args:
        cmp (CampaignEval): The CampaignEval object containing the transactions and product information.

    Returns:
        None
    """
    
    if hasattr(cmp, "custom_upc_details"):
        print("Update transaction with details in custom sku details")    
        cmp_txn_col = cmp.txn.columns
        cmp_product_dim_col = cmp.product_dim.columns
        common_col = set(cmp_txn_col).intersection(cmp_product_dim_col)
        common_col.discard("upc_id")
        print(f"List of column from custom upc details to be replace {list(common_col)}")
        
        cmp.txn = \
        (cmp.txn
            .drop(*common_col)
            .join(cmp.product_dim, 'upc_id', 'left')
            )
    else:
        pass

    return None

def create_period_col(cmp: Union[CampaignEval, CampaignEvalO3]):
    """Create period columns for the CampaignEval object.
    
    This function creates three period columns: period_fis_wk, period_promo_wk, and period_promo_mv_wk. 
    The periods are determined based on the provided CampaignEval object's attributes and flags.
    
    Args:
        cmp (CampaignEval): The CampaignEval object containing relevant information for period column creation.
          
    Returns:
        None
    """
    if cmp.gap_flag:
        cmp.txn = (cmp.txn.withColumn('period_fis_wk',
                                      F.when(F.col('week_id').between(
                                          cmp.cmp_st_wk, cmp.cmp_en_wk), F.lit('dur'))
                                      .when(F.col('week_id').between(cmp.gap_st_wk, cmp.gap_en_wk), F.lit('gap'))
                                      .when(F.col('week_id').between(cmp.pre_st_wk, cmp.pre_en_wk), F.lit('pre'))
                                      .when(F.col('week_id').between(cmp.ppp_st_wk, cmp.ppp_en_wk), F.lit('ppp'))
                                      .otherwise(F.lit('NA')))
                   .withColumn('period_promo_wk',
                               F.when(F.col('promoweek_id').between(
                                   cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('dur'))
                               .when(F.col('promoweek_id').between(cmp.gap_st_promo_wk, cmp.gap_en_promo_wk), F.lit('gap'))
                               .when(F.col('promoweek_id').between(cmp.pre_st_promo_wk, cmp.pre_en_promo_wk), F.lit('pre'))
                               .when(F.col('promoweek_id').between(cmp.ppp_st_promo_wk, cmp.ppp_en_promo_wk), F.lit('ppp'))
                               .otherwise(F.lit('NA')))
                   .withColumn('period_promo_mv_wk',
                               F.when(F.col('promoweek_id').between(
                                   cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('dur'))
                               .when(F.col('promoweek_id').between(cmp.gap_st_promo_wk, cmp.gap_en_promo_wk), F.lit('gap'))
                               .when(F.col('promoweek_id').between(cmp.pre_st_promo_mv_wk, cmp.pre_en_promo_mv_wk), F.lit('pre'))
                               .when(F.col('promoweek_id').between(cmp.ppp_st_promo_mv_wk, cmp.ppp_en_promo_mv_wk), F.lit('ppp'))
                               .otherwise(F.lit('NA')))
                   )
    else:
        cmp.txn = (cmp.txn.withColumn('period_fis_wk',
                                      F.when(F.col('week_id').between(
                                          cmp.cmp_st_wk, cmp.cmp_en_wk), F.lit('dur'))
                                      .when(F.col('week_id').between(cmp.pre_st_wk, cmp.pre_en_wk), F.lit('pre'))
                                      .when(F.col('week_id').between(cmp.ppp_st_wk, cmp.ppp_en_wk), F.lit('ppp'))
                                      .otherwise(F.lit('NA')))
                   .withColumn('period_promo_wk',
                               F.when(F.col('promoweek_id').between(
                                   cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('dur'))
                               .when(F.col('promoweek_id').between(cmp.pre_st_promo_wk, cmp.pre_en_promo_wk), F.lit('pre'))
                               .when(F.col('promoweek_id').between(cmp.ppp_st_promo_wk, cmp.ppp_en_promo_wk), F.lit('ppp'))
                               .otherwise(F.lit('NA')))
                   .withColumn('period_promo_mv_wk',
                               F.when(F.col('promoweek_id').between(
                                   cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('dur'))
                               .when(F.col('promoweek_id').between(cmp.pre_st_promo_mv_wk, cmp.pre_en_promo_mv_wk), F.lit('pre'))
                               .when(F.col('promoweek_id').between(cmp.ppp_st_promo_mv_wk, cmp.ppp_en_promo_mv_wk), F.lit('ppp'))
                               .otherwise(F.lit('NA')))
                   )

    return

def replace_store_region(cmp: Union[CampaignEval, CampaignEvalO3]):
    """Remapping txn store_region follow cmp.store_dim
    """
    cmp.txn = \
        (cmp.txn
         .drop("store_region")
         .join(cmp.store_dim.select("store_id", "store_region"), 'store_id', 'left')
         .fillna('Unidentified', subset='store_region')
        )
    return

def forward_compatible_stored_txn_schema(cmp: Union[CampaignEval, CampaignEvalO3]):
    """Perform forward compatibility adjustments from stored transactions in version 1.
    
    This function ensures backward compatibility with the generated transactions from previous code versions by applying the following adjustments:
    - Change the value in all period columns from 'cmp' to 'dur'.
    - Change the column name 'pkg_weight_unit' to 'units'.
    - Change the column name 'store_format_group' to 'store_format_name'.
    
    Args:
        cmp (CampaignEval): The CampaignEval object containing the stored transactions and necessary information.
        
    Returns:
        None
    """
    if cmp.params["txn_mode"] == "stored_campaign_txn":
        print("Forward compatibility from saved transaction")
        
        cmp.txn = cmp.txn.replace({"cmp":"dur"}, subset=['period_fis_wk', 'period_promo_wk', 'period_promo_mv_wk'])
        
        if "pkg_weight_unit" in cmp.txn.columns:
            cmp.txn = cmp.txn.drop("units").withColumnRenamed("pkg_weight_unit", "units")
            
        if "store_format_group" in cmp.txn.columns:
            cmp.txn = cmp.txn.drop("store_format_name").withColumnRenamed("store_format_group", "store_format_name")
            
        if "channel_flag" in cmp.txn.columns:
            cmp.txn = cmp.txn.drop("offline_online_other_channel").withColumn("offline_online_other_channel", F.col("channel_flag"))
            
    return None

def scope_txn(cmp: Union[CampaignEval, CampaignEvalO3]):
    """Filter and optimize the performance of pre-joined 118-week transactions.
    
    This function improves the performance when using pre-joined 118-week transactions by applying the following steps:
    1. Determine the minimum and maximum week IDs based on the provided campaign evaluation object (`cmp`) and specific parameters.
    2. Add a 1-week buffer to the maximum week ID to account for overflow from Monday to Thursday of the promotional week to the following fiscal week.
    3. Filter the transactions based on the week ID and period columns, retaining only the relevant data.
    
    Args:
        cmp (CampaignEval): The CampaignEval object containing the pre-joined 118-week transactions and relevant information.
        
    Returns:
        None
    """
    ppp_wk_list = [cmp.ppp_st_wk, cmp.ppp_st_promo_wk,
                   cmp.ppp_st_mv_wk, cmp.ppp_st_promo_mv_wk]
    min_wk_id = min([wk for wk in ppp_wk_list if wk is not None])
    cmp_wk_list = [cmp.cmp_en_wk, cmp.cmp_en_promo_wk]
    max_wk_id = max([wk for wk in cmp_wk_list if wk is not None])
    # +1 week buffer for eval with promo week, data of promo week from Mon - Thur will overflow to fis_week + 1
    max_wk_id = period_cal.week_cal(max_wk_id, 1)
    cmp.txn = \
        (cmp.txn
         .where(F.col("week_id").between(min_wk_id, max_wk_id))
         .where((F.col("period_fis_wk").isin(["dur", "gap", "pre", "ppp"])) |
                (F.col("period_promo_wk").isin(["dur", "gap", "pre", "ppp"])) |
                (F.col("period_promo_mv_wk").isin(["dur", "gap", "pre", "ppp"])))
         )
    return

def save_txn(cmp: Union[CampaignEval, CampaignEvalO3]):
    load_txn()
    cmp.txn.write.saveAsTable(
        f"tdm_dev.media_campaign_eval_txn_data_{cmp.params['cmp_id']}")
    return

def get_backward_compatible_txn_schema(cmp: Union[CampaignEval, CampaignEvalO3]):
    """Perform backward compatibility adjustments to the stored transactions.
    
    This function ensures backward compatibility with the generated transactions from previous code versions by applying the following adjustments:
    - Change the value in all period columns from 'dur' to 'cmp'.
    - Change the column name 'unit' to 'pkg_weight_unit'.
    - Change the column name 'store_format_name' to 'store_format_group'.
    
    Args:
        cmp (CampaignEval): The CampaignEval object containing the stored transactions and necessary information.
        
    Returns:
        SparkDataFrame
    """
    back_txn = cmp.txn.replace({"dur":"cmp"}, subset=['period_fis_wk', 'period_promo_wk', 'period_promo_mv_wk'])
        
    if "unit" in cmp.txn.columns:
        back_txn = back_txn.drop("pkg_weight_unit").withColumnRenamed("unit", "pkg_weight_unit")
        
    if "store_format_name" in cmp.txn.columns:
        back_txn = back_txn.drop("store_format_group").withColumnRenamed("store_format_name", "store_format_group")
    
    return back_txn