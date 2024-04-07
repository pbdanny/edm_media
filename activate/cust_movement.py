import pprint
from ast import literal_eval
from typing import List, Union
from datetime import datetime, timedelta
import sys
import os

import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval, CampaignEvalO3
from utils import period_cal
from exposure.exposed import create_txn_offline_x_aisle_target_store
from activate import activated


def get_sku_activated_cust_movement(cmp):
    """Customer movement based on tagged feature activated & brand activated"""
    cmp.spark.sparkContext.setCheckpointDir(
        "dbfs:/mnt/pvtdmbobazc01/edminput/filestore/user/thanakrit_boo/tmp/checkpoint"
    )
    txn = cmp.txn
    feat_sf = cmp.feat_sku
    class_df = cmp.feat_class_sku
    sclass_df = cmp.feat_subclass_sku
    brand_df = cmp.feat_brand_sku
    switching_lv = cmp.params["cate_lvl"]

    cust_purchased_exposure_count = activated.get_cust_by_mech_exposed_purchased(
        cmp, prd_scope_df=cmp.feat_sku, prd_scope_nm="sku"
    )

    sku_activated = cust_purchased_exposure_count.select(
        "household_id"
    ).drop_duplicates()

    if hasattr(cmp, "sku_activated_cust_movement"):
        print("Campaign object already have attribute 'sku_activated_cust_movement'")
        return

    try:
        cmp.sku_activated_cust_movement = cmp.spark.table(
            f"tdm_dev.media_camp_eval_{cmp.params['cmp_id']}_cust_mv"
        )
        print(
            f"Load 'matched_store' from tdm_dev.media_camp_eval_f{cmp.params['cmp_id']}_cust_mv"
        )
        return
    except Exception as e:
        print(e)
        pass

    # ---- Main
    # Movement
    # Existing and New SKU buyer (movement at micro level)
    print("-" * 80)
    print("Customer movement")
    print("Movement consider only Feature SKU activated")
    print("-" * 80)

    print("-" * 80)
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col_nm}")
    print("-" * 80)

    # Features SKU movement
    prior_pre_sku_shopper = (
        txn.where(F.col("household_id").isNotNull())
        .where(F.col(period_wk_col_nm).isin(["pre", "ppp"]))
        .join(feat_sf, "upc_id", "inner")
        .select("household_id")
        .drop_duplicates()
    )

    existing_exposed_cust_and_sku_shopper = (
        sku_activated.select("household_id")
        .join(prior_pre_sku_shopper, "household_id", "inner")
        .withColumn("customer_macro_flag", F.lit("existing"))
        .withColumn("customer_micro_flag", F.lit("existing_sku"))
        .checkpoint()
    )

    new_exposed_cust_and_sku_shopper = (
        sku_activated.select("household_id")
        .join(existing_exposed_cust_and_sku_shopper, "household_id", "leftanti")
        .withColumn("customer_macro_flag", F.lit("new"))
        .checkpoint()
    )

    # Customer movement for Feature SKU
    ## Macro level (New/Existing/Lapse)
    prior_pre_cc_txn = txn.where(F.col("household_id").isNotNull()).where(
        F.col(period_wk_col_nm).isin(["pre", "ppp"])
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select("household_id").drop_duplicates()

    prior_pre_class_shopper = (
        prior_pre_cc_txn.join(class_df, "upc_id", "inner")
        .select("household_id")
        .drop_duplicates()
    )

    prior_pre_subclass_shopper = (
        prior_pre_cc_txn.join(sclass_df, "upc_id", "inner")
        .select("household_id")
        .drop_duplicates()
    )

    ## Micro level
    new_sku_new_store = (
        new_exposed_cust_and_sku_shopper.join(
            prior_pre_store_shopper, "household_id", "leftanti"
        )
        .select("household_id", "customer_macro_flag")
        .withColumn("customer_micro_flag", F.lit("new_to_lotus"))
    )

    new_sku_new_class = (
        new_exposed_cust_and_sku_shopper.join(
            prior_pre_store_shopper, "household_id", "inner"
        )
        .join(prior_pre_class_shopper, "household_id", "leftanti")
        .select("household_id", "customer_macro_flag")
        .withColumn("customer_micro_flag", F.lit("new_to_class"))
    )

    if switching_lv == "subclass":
        new_sku_new_subclass = (
            new_exposed_cust_and_sku_shopper.join(
                prior_pre_store_shopper, "household_id", "inner"
            )
            .join(prior_pre_class_shopper, "household_id", "inner")
            .join(prior_pre_subclass_shopper, "household_id", "leftanti")
            .select("household_id", "customer_macro_flag")
            .withColumn("customer_micro_flag", F.lit("new_to_subclass"))
        )

        prior_pre_brand_in_subclass_shopper = (
            prior_pre_cc_txn.join(sclass_df, "upc_id", "inner")
            .join(brand_df, "upc_id")
            .select("household_id")
            .drop_duplicates()
        )

        # ---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = (
            new_exposed_cust_and_sku_shopper.join(
                prior_pre_store_shopper, "household_id", "inner"
            )
            .join(prior_pre_class_shopper, "household_id", "inner")
            .join(prior_pre_subclass_shopper, "household_id", "inner")
            .join(prior_pre_brand_in_subclass_shopper, "household_id", "leftanti")
            .select("household_id", "customer_macro_flag")
            .withColumn("customer_micro_flag", F.lit("new_to_brand"))
        )

        new_sku_within_brand_shopper = (
            new_exposed_cust_and_sku_shopper.join(
                prior_pre_store_shopper, "household_id", "inner"
            )
            .join(prior_pre_class_shopper, "household_id", "inner")
            .join(prior_pre_subclass_shopper, "household_id", "inner")
            .join(prior_pre_brand_in_subclass_shopper, "household_id", "inner")
            .join(prior_pre_sku_shopper, "household_id", "leftanti")
            .select("household_id", "customer_macro_flag")
            .withColumn("customer_micro_flag", F.lit("new_to_sku"))
        )

        result_movement = (
            existing_exposed_cust_and_sku_shopper.unionByName(new_sku_new_store)
            .unionByName(new_sku_new_class)
            .unionByName(new_sku_new_subclass)
            .unionByName(new_sku_new_brand_shopper)
            .unionByName(new_sku_within_brand_shopper)
            .checkpoint()
        )
        cmp.activated_cust_movement = result_movement
        return result_movement, new_exposed_cust_and_sku_shopper

    elif switching_lv == "class":
        prior_pre_brand_in_class_shopper = (
            prior_pre_cc_txn.join(class_df, "upc_id", "inner")
            .join(brand_df, "upc_id")
            .select("household_id")
        ).drop_duplicates()

        # ---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = (
            new_exposed_cust_and_sku_shopper.join(
                prior_pre_store_shopper, "household_id", "inner"
            )
            .join(prior_pre_class_shopper, "household_id", "inner")
            .join(prior_pre_brand_in_class_shopper, "household_id", "leftanti")
            .select("household_id", "customer_macro_flag")
            .withColumn("customer_micro_flag", F.lit("new_to_brand"))
        )

        new_sku_within_brand_shopper = (
            new_exposed_cust_and_sku_shopper.join(
                prior_pre_store_shopper, "household_id", "inner"
            )
            .join(prior_pre_class_shopper, "household_id", "inner")
            .join(prior_pre_brand_in_class_shopper, "household_id", "inner")
            .join(prior_pre_sku_shopper, "household_id", "leftanti")
            .select("household_id", "customer_macro_flag")
            .withColumn("customer_micro_flag", F.lit("new_to_sku"))
        )

        result_movement = (
            existing_exposed_cust_and_sku_shopper.unionByName(new_sku_new_store)
            .unionByName(new_sku_new_class)
            .unionByName(new_sku_new_brand_shopper)
            .unionByName(new_sku_within_brand_shopper)
            .checkpoint()
        )
        cmp.sku_activated_cust_movement = result_movement
        return result_movement, new_exposed_cust_and_sku_shopper

    else:
        print("Not recognized Movement and Switching level param")
        return None, None
