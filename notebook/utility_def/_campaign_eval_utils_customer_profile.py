# Databricks notebook source
# MAGIC %md
# MAGIC # Import all library

# COMMAND ----------

## Pat Add import all lib here -- 25 May 2022
## import function
##---------------------------------------------------
## Need to import all function in each notebook
##---------------------------------------------------

## import pyspark sql

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

from pyspark import StorageLevel


## import longging and traceback
import logging
import traceback
import errno

## datetime lib
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
import time

## pandas and numpy
import pandas as pd
import numpy as np
import math as math

## os path
import os
import sys
import string
import subprocess
import importlib
import shutil
import urllib 
import pathlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

# COMMAND ----------


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
            date_dim = spark.table("tdm.v_th_date_dim")
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
    return idx_tp

