# Databricks notebook source
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

# COMMAND ----------

# MAGIC %run /EDM_Share/EDM_Media/Campaign_Evaluation/Instore/utility_def/edm_utils

# COMMAND ----------

def get_sales_mkt_growth_per_mech(
    txn: SparkDataFrame,
    week_type: str,
    store_format: str,
    trg_store_df: SparkDataFrame,
    cmp_shppr_last_seen_brand_tag: SparkDataFrame,
    cmp_shppr_last_seen_sku_tag: SparkDataFrame,
) -> SparkDataFrame:
    """Get sales and marketing metrics for each mechanic.

    Parameters:
    -----------
    txn: DataFrame
        Snapped transaction of ppp + pre + cmp period
    week_type: str
        "fis_week" or "promo_week"
    store_format: str
        Store format type
    trg_store_df: DataFrame
        Target stores.
    cmp_shppr_last_seen_brand_tag: DataFrame
        cmp shopper tagging
    cmp_shppr_last_seen_sku_tag: DataFrame
        cmp shopper tagging

    return: SparkDataFrame
        Returns a spark DF of sales and marketing metrics by mechanic
    """
    txn,
    week_type,
    store_format,
    trg_store_df,
    cmp_shppr_last_seen_brand_tag,
    cmp_shppr_last_seen_sku_tag,
):
    """Get customer exposed & unexposed / shopped, not shop

    Parameters
    ----------
    txn:
        Snapped transaction of ppp + pre + cmp period
    wk_type:
        "fis_week" or "promo_week"
    trg_store_df:
    """
    # get mechanic list
    mechanic_list = (
        trg_store_df.select("mech_name")
        .drop_duplicates()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    print("List of detected mechanics from store list: ", mechanic_list)

    num_of_mechanics = len(mechanic_list)

    """
    cmp_shppr_last_seen_brand_tag from get_cust_activated_by_mech
    +------------------+----------------------------------------+-----+-----------------------+
    |      household_id|Cross_Category_ShelfDivider_With_Product|level|total_mechanics_exposed|
    +------------------+----------------------------------------+-----+-----------------------+
    |102111060007422619|                                       1|brand|                      1|
    |102111060002777844|                                       1|brand|                      1|
    |102111060007893466|                                       1|brand|                      1|
    |102111060002076397|                                       1|brand|                      1|
    |102111060001202275|                                       1|brand|                      1|
    |102111060002128083|                                       1|brand|                      1|
    |106111060027098574|                                       1|brand|                      1|
    |102111060007612077|                                       1|brand|                      1|
    |102111060008552583|                                       1|brand|                      1|
    |102111060018514635|                                       1|brand|                      1|
    +------------------+----------------------------------------+-----+-----------------------+
    """
    if store_format == "":
        print("Not define store format, will use HDE as default")
        store_format = "HDE"
    # end if

    # Select week period column name
    if week_type == "fis_wk":
        period_col = "period_fis_wk"
        week_col = "week_id"
    elif week_type == "promo_wk":
        period_col = "period_promo_wk"
        week_col = "promoweek_id"
    else:
        print("In correct week_type, Will use fis_week as default value \n")
        period_col = "period_fis_wk"
        week_col = "week_id"
    # filter txn for category (class) level and brand level
    txn_pre_cond = """ ({0} == 'pre') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(
        period_col, store_format
    )

    txn_dur_cond = """ ({0} == 'cmp') and 
                       ( lower(store_format_online_subchannel_other) == '{1}') and
                       ( offline_online_other_channel == 'OFFLINE')
                   """.format(
        period_col, store_format
    )
    # join with mech
    # add brand
    txn_mech_pre_trg_brand = (
        txn.join(cmp_shppr_last_seen_brand_tag, "household_id", "left_semi")
        .join(trg_store_df, [txn.store_id == trg_store_df.store_id], "left_semi")
        .where(txn_pre_cond)
    )

    txn_mech_dur_trg_brand = (
        txn.join(cmp_shppr_last_seen_brand_tag, "household_id", "left_semi")
        .join(trg_store_df, [txn.store_id == trg_store_df.store_id], "left_semi")
        .where(txn_dur_cond)
    )
    # add sku
    txn_mech_pre_trg_sku = (
        txn.join(cmp_shppr_last_seen_sku_tag, "household_id", "left_semi")
        .join(trg_store_df, [txn.store_id == trg_store_df.store_id], "left_semi")
        .where(txn_pre_cond)
    )

    txn_mech_dur_trg_sku = (
        txn.join(cmp_shppr_last_seen_sku_tag, "household_id", "left_semi")
        .join(trg_store_df, [txn.store_id == trg_store_df.store_id], "left_semi")
        .where(txn_dur_cond)
    )

    def _get_growth(txn_mech_pre_trg_brand, txn_mech_pre_trg_sku,week_col=week_col):
        # for fis_week
        if week_col == "week_id":

            # target brand
            brand_pre_trg_sum = txn_mech_pre_trg_brand.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            brand_dur_trg_sum = txn_mech_dur_trg_brand.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
            # target sku
            sku_pre_trg_sum = txn_mech_pre_trg_sku.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            sku_dur_trg_sum = txn_mech_dur_trg_sku.groupBy(
                F.col("week_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
        else:  # use promo week

            # target brand
            brand_pre_trg_sum = txn_mech_pre_trg_brand.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            brand_dur_trg_sum = txn_mech_dur_trg_brand.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
            # target sku
            sku_pre_trg_sum = txn_mech_pre_trg_sku.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))

            sku_dur_trg_sum = txn_mech_dur_trg_sku.groupBy(
                F.col("promoweek_id").alias("period_wk")
            ).agg(F.sum("net_spend_amt").alias("sales"))
        # end if

        # target
        # average in each period
        brand_pre_trg_agg = brand_pre_trg_sum.agg(
            F.avg("sales").alias("brand_pre_trg_avg"),
            sum("sales").alias("brand_pre_trg_ssum"),
        )
        brand_dur_trg_agg = brand_dur_trg_sum.agg(
            F.avg("sales").alias("brand_dur_trg_avg"),
            sum("sales").alias("brand_dur_trg_ssum"),
        )
        sku_pre_trg_agg = sku_pre_trg_sum.agg(
            F.avg("sales").alias("sku_pre_trg_avg"),
            sum("sales").alias("sku_pre_trg_ssum"),
        )
        sku_dur_trg_agg = sku_dur_trg_sum.agg(
            F.avg("sales").alias("sku_dur_trg_avg"),
            sum("sales").alias("sku_dur_trg_ssum"),
        )

        brand_pre_trg_ssum, brand_pre_trg_avg = brand_pre_trg_agg.select(
            "brand_pre_trg_ssum", "brand_pre_trg_avg"
        ).collect()[0]
        brand_dur_trg_ssum, brand_dur_trg_avg = brand_dur_trg_agg.select(
            "brand_dur_trg_ssum", "brand_dur_trg_avg"
        ).collect()[0]
        sku_pre_trg_ssum, sku_pre_trg_avg = sku_pre_trg_agg.select(
            "sku_pre_trg_ssum", "sku_pre_trg_avg"
        ).collect()[0]
        sku_dur_trg_ssum, sku_dur_trg_avg = sku_dur_trg_agg.select(
            "sku_dur_trg_ssum", "sku_dur_trg_avg"
        ).collect()[0]

        del brand_pre_trg_agg
        del brand_dur_trg_agg
        del sku_pre_trg_agg
        del sku_dur_trg_agg
        # --------------------------------------
        # get sales growth pre vs during -> use average weekly Product level test vs control
        # --------------------------------------

        if sku_pre_trg_avg is None:
            print(' "sku_pre_trg_avg" has no value, will set value to = 0 \n ')
            sku_pre_trg_avg = 0
        # end if
        trg_wkly_growth = div_zero_chk(
            (sku_dur_trg_avg - sku_pre_trg_avg), sku_pre_trg_avg
        )

        if brand_pre_trg_avg is None:
            print(' "brand_pre_trg_avg" has no value, will set value to = 0 \n ')
            brand_pre_trg_avg = 0
        # end if

        brand_wkly_growth = div_zero_chk(
            (brand_dur_trg_avg - brand_pre_trg_avg), brand_pre_trg_avg
        )

        trg_mkt_pre = div_zero_chk(sku_pre_trg_ssum, brand_pre_trg_ssum)
        trg_mkr_dur = div_zero_chk(sku_dur_trg_ssum, brand_dur_trg_ssum)
        trg_mkt_growth = div_zero_chk((trg_mkr_dur - trg_mkt_pre), trg_mkt_pre)

        dummy = 0 / brand_pre_trg_ssum

        pd_cols = ["level", "str_fmt", "pre", "during", "week_type"]
        pd_data = [
            ["brand_wk_avg", "Test", brand_pre_trg_avg, brand_dur_trg_avg, week_col],
            ["brand_wk_avg", "Test", brand_pre_trg_avg, brand_dur_trg_avg, week_col],
            ["sku_wk_grw", "Test", dummy, brand_wkly_growth, week_col],
            ["sku_wk_grw", "Test", dummy, trg_wkly_growth, week_col],
            ["MKT_SHARE", "Test", trg_mkt_pre, trg_mkr_dur, week_col],
            ["brand_sales", "Test", brand_pre_trg_ssum, brand_dur_trg_ssum, week_col],
            ["brand_sales", "Test", brand_pre_trg_ssum, brand_dur_trg_ssum, week_col],
            ["mkt_growth", "Test", dummy, trg_mkt_growth, week_col],
        ]

        sales_info_pd = pd.DataFrame(pd_data, columns=pd_cols)

        return spark.createDataFrame(sales_info_pd) 

    mech_growth = _get_growth(txn_mech_pre_trg_brand, txn_mech_pre_trg_sku).withColumn(
        "mechanic", F.lit("all")
    )
    # Add by mech if there are more than 1 mechanic
    if num_of_mechanics > 1:
        mech_results = {}

        for mech in mechanic_list:
            mech_results[mech] = _get_growth(
                txn_mech_pre_trg_brand.filter(F.col(mech) == 1),
                txn_mech_pre_trg_sku.filter(F.col(mech) == 1),
            ).withColumn("mechanic", F.lit(mech))

            mech_growth = mech_growth.unionByName(mech_results[mech])
    return mech_growth
