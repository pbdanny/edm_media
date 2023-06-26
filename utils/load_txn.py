import pprint
from ast import literal_eval
from typing import List
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
from utils.campaign_config import CampaignEval
from utils import period_cal

sys.path.append(os.path.abspath(
    "/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
from edm_class import txnItem

def load_txn(cmp: CampaignEval,
             txn_mode: str = "pre_generated_118wk"):
    """Load transaction

    Parameters
    ----------
    txn_mode: str, default = "pre_generated_118wk"
        "pre_generated_118wk" : load from pregenerated tdm_seg.v_latest_txn118wk
        "campaign_specific" : load from created tdm_seg.media_campaign_eval_txn_data_{cmp.params['cmp_id']}
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

    elif txn_mode == "campaign_specific":
        try:
            cmp.txn = cmp.spark.table(
                f"tdm_seg.media_campaign_eval_txn_data_{cmp.params['cmp_id'].lower()}")
            cmp.params["txn_mode"] = "campaign_specific"
        except Exception as e:
            cmp.params["txn_mode"] = "pre_generated_118wk"
            cmp.txn = cmp.spark.table("tdm_seg.v_latest_txn118wk")
    else:
        cmp.params["txn_mode"] = "pre_generated_118wk"
        cmp.txn = cmp.spark.table("tdm_seg.v_latest_txn118wk")

    backward_compate_legacy_stored_txn(cmp)
    create_period_col(cmp)
    scope_txn(cmp)
    replace_brand_nm(cmp)
    replace_store_region(cmp)

    return

def replace_brand_nm(cmp: CampaignEval):
    """Replace txn of multi feature brand with first main brand
    """
    cmp.txn = \
    (cmp.txn
        .drop("brand_name")
        .join(cmp.product_dim.select("upc_id", "brand_name"), 'upc_id', 'left')
        .fillna('Unidentified', subset='brand_name')
    )

    return

def create_period_col(cmp: CampaignEval):
    """Create period columns : period_fis_wk, period_promo_wk, period_promo_mv_wk
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

def replace_store_region(cmp: CampaignEval):
    """Remapping txn store_region follow cmp.store_dim
    """
    cmp.txn = \
        (cmp.txn
         .drop("store_region")
         .join(cmp.store_dim.select("store_id", "store_region"), 'store_id', 'left')
         .fillna('Unidentified', subset='store_region')
        )
    return

def backward_compate_legacy_stored_txn(cmp: CampaignEval):
    """Backward compatibility with generated txn from code
    - Change value in all period columns from 'cmp' -> 'dur'
    - Change column name 'pkg_weight_unit' -> 'unit'
    """
    cmp.txn = cmp.txn.replace({"cmp":"dur"}, subset=['period_fis_wk', 'period_promo_wk', 'period_promo_mv_wk'])
        
    if "pkg_weight_unit" in cmp.txn.columns:
        cmp.txn = cmp.txn.drop("unit").withColumnRenamed("pkg_weight_unit", "unit")

    return

def scope_txn(cmp: CampaignEval):
    """Improve performance when use pre-joined 118wk txn
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

def save_txn(cmp: CampaignEval):
    load_txn()
    cmp.txn.write.saveAsTable(
        f"tdm_seg.media_campaign_eval_txn_data_{cmp.params['cmp_id']}")
    return
