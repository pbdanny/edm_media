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

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))

from edm_class import txnItem

spark = SparkSession.builder.appName("campaingEval").getOrCreate()

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
                       head_col_select=["transaction_uid", "date_id", "store_id", "channel", "pos_type", "pos_id"],
                       item_col_select=['transaction_uid', 'store_id', 'date_id', 'upc_id', 'week_id', 
                                        'net_spend_amt', 'unit', 'customer_id', 'discount_amt', 'cc_flag'],
                       prod_col_select=['upc_id', 'division_name', 'department_name', 'section_id', 'section_name', 
                                        'class_id', 'class_name', 'subclass_id', 'subclass_name', 'brand_name',
                                        'department_code', 'section_code', 'class_code', 'subclass_code'])

        cmp.txn = snap.txn
        
    elif txn_mode == "campaign_specific":
        try:
            cmp.txn = spark.table(f"tdm_seg.media_campaign_eval_txn_data_{cmp.params['cmp_id']}")
            cmp.params["txn_mode"] = "campaign_specific"
        except Exception as e:
            cmp.params["txn_mode"] = "pre_generated_118wk"
            cmp.txn = spark.table("tdm_seg.v_latest_txn118wk")
    else:
        cmp.params["txn_mode"] = "pre_generated_118wk"
        cmp.txn = spark.table("tdm_seg.v_latest_txn118wk")

    create_period_col(cmp)
    cmp.txn = cmp.txn.where( (F.col("period_fis_wk").isin(["cmp", "gap", "pre", "ppp"])) | 
                            (F.col("period_promo_wk").isin(["cmp", "gap", "pre", "ppp"])) |
                            (F.col("period_promo_mv_wk").isin(["cmp", "gap", "pre", "ppp"])) )
    replace_brand_nm(cmp)
    combine_store_region(cmp)
    
    pass

def replace_brand_nm(cmp: CampaignEval):
    """Replace muliti feature brand with main brand
    """
    brand_list = cmp.feat_brand_nm.toPandas()["brand_name"].tolist()
    brand_list.sort()
    if len(brand_list) > 1:
        cmp.txn = cmp.txn.withColumn("brand_name", F.when(F.col("brand_name").isin(brand_list), F.lit(brand_list[0])).otherwise(F.col("brand_name")))
    pass

def create_period_col(cmp: CampaignEval):
    """Create period columns : period_fis_wk, period_promo_wk, period_promo_mv_wk
    """
    if cmp.gap_flag:
        cmp.txn = (cmp.txn.withColumn('period_fis_wk', 
                            F.when(F.col('week_id').between(cmp.cmp_st_wk, cmp.cmp_en_wk), F.lit('cmp'))
                             .when(F.col('week_id').between(cmp.gap_st_wk, cmp.gap_en_wk), F.lit('gap'))
                             .when(F.col('week_id').between(cmp.pre_st_wk, cmp.pre_en_wk), F.lit('pre'))
                             .when(F.col('week_id').between(cmp.ppp_st_wk, cmp.ppp_en_wk), F.lit('ppp'))
                             .otherwise(F.lit('NA')))
                           .withColumn('period_promo_wk', 
                            F.when(F.col('promoweek_id').between(cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('cmp'))
                             .when(F.col('promoweek_id').between(cmp.gap_st_promo_wk, cmp.gap_en_promo_wk), F.lit('gap'))
                             .when(F.col('promoweek_id').between(cmp.pre_st_promo_wk, cmp.pre_en_promo_wk), F.lit('pre'))
                             .when(F.col('promoweek_id').between(cmp.ppp_st_promo_wk, cmp.ppp_en_promo_wk), F.lit('ppp'))
                             .otherwise(F.lit('NA')))
                           .withColumn('period_promo_mv_wk', 
                            F.when(F.col('promoweek_id').between(cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('cmp'))
                            .when(F.col('promoweek_id').between(cmp.gap_st_promo_wk, cmp.gap_en_promo_wk), F.lit('gap'))
                            .when(F.col('promoweek_id').between(cmp.pre_st_promo_mv_wk, cmp.pre_en_promo_mv_wk), F.lit('pre'))
                            .when(F.col('promoweek_id').between(cmp.ppp_st_promo_mv_wk, cmp.ppp_en_promo_mv_wk), F.lit('ppp'))
                            .otherwise(F.lit('NA')))
                        )
    else:
        cmp.txn = (cmp.txn.withColumn('period_fis_wk', 
                            F.when(F.col('week_id').between(cmp.cmp_st_wk, cmp.cmp_en_wk), F.lit('cmp'))
                            .when(F.col('week_id').between(cmp.pre_st_wk, cmp.pre_en_wk), F.lit('pre'))
                            .when(F.col('week_id').between(cmp.ppp_st_wk, cmp.ppp_en_wk), F.lit('ppp'))
                            .otherwise(F.lit('NA')))
                          .withColumn('period_promo_wk', 
                            F.when(F.col('promoweek_id').between(cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('cmp'))
                             .when(F.col('promoweek_id').between(cmp.pre_st_promo_wk, cmp.pre_en_promo_wk), F.lit('pre'))
                             .when(F.col('promoweek_id').between(cmp.ppp_st_promo_wk, cmp.ppp_en_promo_wk), F.lit('ppp'))
                             .otherwise(F.lit('NA')))
                          .withColumn('period_promo_mv_wk', 
                            F.when(F.col('promoweek_id').between(cmp.cmp_st_promo_wk, cmp.cmp_en_promo_wk), F.lit('cmp'))
                            .when(F.col('promoweek_id').between(cmp.pre_st_promo_mv_wk, cmp.pre_en_promo_mv_wk), F.lit('pre'))
                            .when(F.col('promoweek_id').between(cmp.ppp_st_promo_mv_wk, cmp.ppp_en_promo_mv_wk), F.lit('ppp'))
                            .otherwise(F.lit('NA')))
        )
        
    pass

def combine_store_region(cmp: CampaignEval):
    """For Gofresh, reclassified West , Central -> West+Central
    """
    if cmp.store_fmt in ["gofresh", "mini_super"]:
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
            
            cmp.txn = cmp.txn.drop('store_region').join(adjusted_store_region, 'store_id', 'left').when(F.col('region').isNull(), F.lit('Unidentified'))    
    pass

def save_txn(cmp: CampaignEval):
    load_txn()
    cmp.txn.write.saveAsTable(f"tdm_seg.media_campaign_eval_txn_data_{cmp.params['cmp_id']}")
    pass