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

from exposure import exposed
from utils import period_cal
from matching import store_matching

from functools import reduce

#---- Helper fn
def union_frame(left, right):
    return left.unionByName(right, allowMissingColumns=True)

#---- target dur
def get_txn_target_store_aisle_cross_cate_dur(cmp: CampaignEval):
    """
    """
    exposed.create_txn_offline_x_aisle_target_store(cmp)
    txn_aisle_cross_cate = \
        (cmp.txn_offline_x_aisle_target_store
         .where(F.col("aisle_scope").isin(["cross_cate"]))
         )
    return txn_aisle_cross_cate

def get_txn_target_store_feature_dur(cmp: CampaignEval,
                                  prd_scope_df: SparkDataFrame):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    txn_target_store_feature = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(cmp.target_store.select("store_id").drop_duplicates(), "store_id")
         .where(F.col(period_wk_col_nm).isin(["dur"]))
         .join(prd_scope_df, 'upc_id')  
        )
    return txn_target_store_feature

def get_txn_target_store_total_dur(cmp: CampaignEval):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    txn_target_store = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(cmp.target_store.select("store_id").drop_duplicates(), "store_id")
         .where(F.col(period_wk_col_nm).isin(["dur"]))
        )
    return txn_target_store

def get_bask_asso_target_dur(cmp: CampaignEval,
                  prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_target_store_aisle_cross_cate_dur(cmp)
    feat_txn = get_txn_target_store_feature_dur(cmp, prd_scope_df)

    bask_asso = (aisle_txn
                 .select("transaction_uid").drop_duplicates()
                 .join(feat_txn.select("transaction_uid").drop_duplicates(), 
                       "transaction_uid", "inner")
                 .select("transaction_uid")
                 ).drop_duplicates()

    return bask_asso

def asso_size_target_dur(cmp: CampaignEval,
              prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_target_store_aisle_cross_cate_dur(cmp)
    feat_txn = get_txn_target_store_feature_dur(cmp, prd_scope_df)
    bask_asso = get_bask_asso_target_dur(cmp, prd_scope_df)
    total_txn = get_txn_target_store_total_dur(cmp)

    size_feat_in_asso = \
        (feat_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        .withColumn("bask_type", F.lit("feat_in_asso"))
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))

        )

    size_aisle_in_asso = \
        (aisle_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("aisle_in_asso"))

        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
        )
    total_store = \
        (total_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("total"))
        )
    total_store_visits = total_store.select("visits").collect()[0][0]
        
    combine = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle, total_store])
    combine_add_col = (combine
                       .withColumn("period", F.lit("dur"))
                       .withColumn("store_type", F.lit("test"))
                       .withColumn("visit_pen", F.col("visits")/total_store_visits)
    )
    
    return combine_add_col

#---- target pre
def get_txn_target_store_aisle_cross_cate_pre(cmp: CampaignEval):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)
    cross_cate_target_store = \
        (cmp.aisle_target_store_conf
         .where(F.col("aisle_scope").isin(["cross_cate"]))
         .select("store_id", "upc_id")
         ).drop_duplicates()

    txn_aisle_cross_cate_pre = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .where(F.col(period_wk_col_nm).isin(["pre"]))
         .join(cross_cate_target_store, ["store_id", "upc_id"])
         )
    return txn_aisle_cross_cate_pre

def get_txn_target_store_feature_pre(cmp: CampaignEval,
                                     prd_scope_df: SparkDataFrame):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    txn_target_store_feature_pre = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(cmp.target_store.select("store_id").drop_duplicates(), "store_id")
         .where(F.col(period_wk_col_nm).isin(["pre"]))
         .join(prd_scope_df, 'upc_id')  
        )
    return txn_target_store_feature_pre

def get_txn_target_store_total_pre(cmp: CampaignEval,):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    txn_target_store = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(cmp.target_store.select("store_id").drop_duplicates(), "store_id")
         .where(F.col(period_wk_col_nm).isin(["pre"]))
        )
    return txn_target_store

def get_bask_target_asso_pre(cmp: CampaignEval,
                  prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_target_store_aisle_cross_cate_pre(cmp)
    feat_txn = get_txn_target_store_feature_pre(cmp, prd_scope_df)

    bask_asso = (aisle_txn
                 .select("transaction_uid").drop_duplicates()
                 .join(feat_txn.select("transaction_uid").drop_duplicates(), 
                       "transaction_uid", "inner")
                 .select("transaction_uid")
                 ).drop_duplicates()

    return bask_asso

def asso_size_target_pre(cmp: CampaignEval,
                  prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_target_store_aisle_cross_cate_pre(cmp)
    feat_txn = get_txn_target_store_feature_pre(cmp, prd_scope_df)
    bask_asso = get_bask_target_asso_pre(cmp, prd_scope_df)
    total_txn = get_txn_target_store_total_pre(cmp)


    size_feat_in_asso = \
        (feat_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        .withColumn("bask_type", F.lit("feat_in_asso"))
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
        )

    size_aisle_in_asso = \
        (aisle_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("aisle_in_asso"))
        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
        )
        
    total_store = \
        (total_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("total"))
        )
    total_store_visits = total_store.select("visits").collect()[0][0]

    combine = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle, total_store])
    combine_add_col = (combine.withColumn("period", F.lit("pre"))
                       .withColumn("store_type", F.lit("test"))
                       .withColumn("visit_pen", F.col("visits")/total_store_visits)

    )

    return combine_add_col

#---- ctrl dur
def get_txn_ctrl_store_aisle_cross_cate_dur(cmp: CampaignEval):
    """
    """
    store_matching.get_store_matching_across_region(cmp)
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)
    
    cross_cate_target_store = \
        (cmp.aisle_target_store_conf
        .where(F.col("aisle_scope").isin(["cross_cate"]))
        .select("store_id", "upc_id")
        ).drop_duplicates()

    matched_store_id = \
        (cmp.matched_store
        .select("test_store_id", "ctrl_store_id")
        .drop_duplicates()
        )

    cross_cate_ctrl_store = \
        (matched_store_id
        .withColumnRenamed("test_store_id", "store_id")
        .join(cross_cate_target_store, "store_id", "left")
        .drop("store_id")
        .withColumnRenamed("ctrl_store_id", "store_id")
        )

    txn_aisle_cross_cate_ctrl = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .where(F.col(period_wk_col_nm).isin(["dur"]))
         .join(cross_cate_ctrl_store, ["store_id", "upc_id"])
         )
    
    return txn_aisle_cross_cate_ctrl

def get_txn_ctrl_store_feature_dur(cmp: CampaignEval,
                               prd_scope_df: SparkDataFrame):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    matched_ctrl_store_id = \
        (cmp.matched_store
        .select("ctrl_store_id")
        .drop_duplicates()
        .withColumnRenamed("ctrl_store_id", "store_id")
        )

    txn_ctrl_store_feature = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(matched_ctrl_store_id, "store_id")
         .where(F.col(period_wk_col_nm).isin(["dur"]))
         .join(prd_scope_df, 'upc_id')  
        )

    return txn_ctrl_store_feature

def get_txn_ctrl_store_total_dur(cmp: CampaignEval):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    matched_ctrl_store_id = \
        (cmp.matched_store
        .select("ctrl_store_id")
        .drop_duplicates()
        .withColumnRenamed("ctrl_store_id", "store_id")
        )

    txn_ctrl_store = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(matched_ctrl_store_id, "store_id")
         .where(F.col(period_wk_col_nm).isin(["dur"]))
        )

    return txn_ctrl_store

def get_bask_asso_ctrl_dur(cmp: CampaignEval,
                       prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_ctrl_store_aisle_cross_cate_dur(cmp)
    feat_txn = get_txn_ctrl_store_feature_dur(cmp, prd_scope_df)

    bask_asso = (aisle_txn
                 .select("transaction_uid").drop_duplicates()
                 .join(feat_txn.select("transaction_uid").drop_duplicates(), 
                       "transaction_uid", "inner")
                 .select("transaction_uid")
                 ).drop_duplicates()

    return bask_asso

def asso_size_ctrl_dur(cmp: CampaignEval,
              prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_ctrl_store_aisle_cross_cate_dur(cmp)
    feat_txn = get_txn_ctrl_store_feature_dur(cmp, prd_scope_df)
    bask_asso = get_bask_asso_ctrl_dur(cmp, prd_scope_df)
    total_txn = get_txn_ctrl_store_total_dur(cmp)

    size_feat_in_asso = \
        (feat_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        .withColumn("bask_type", F.lit("feat_in_asso"))
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
        )

    size_aisle_in_asso = \
        (aisle_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("aisle_in_asso"))
        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
        )

    total_store = \
        (total_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("total"))
        )
    total_store_visits = total_store.select("visits").collect()[0][0]

    combine = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle, total_store])
    combine_add_col = (combine.withColumn("period", F.lit("dur"))
                       .withColumn("store_type", F.lit("ctrl"))
                       .withColumn("visit_pen", F.col("visits")/total_store_visits)
    )

    return combine_add_col

#---- ctrl pre
def get_txn_ctrl_store_aisle_cross_cate_pre(cmp: CampaignEval):
    """
    """
    store_matching.get_store_matching_across_region(cmp)
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)
    
    cross_cate_target_store = \
        (cmp.aisle_target_store_conf
        .where(F.col("aisle_scope").isin(["cross_cate"]))
        .select("store_id", "upc_id")
        ).drop_duplicates()

    matched_store_id = \
        (cmp.matched_store
        .select("test_store_id", "ctrl_store_id")
        .drop_duplicates()
        )

    cross_cate_ctrl_store = \
        (matched_store_id
        .withColumnRenamed("test_store_id", "store_id")
        .join(cross_cate_target_store, "store_id", "left")
        .drop("store_id")
        .withColumnRenamed("ctrl_store_id", "store_id")
        )

    txn_aisle_cross_cate_ctrl = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .where(F.col(period_wk_col_nm).isin(["pre"]))
         .join(cross_cate_ctrl_store, ["store_id", "upc_id"])
         )
    
    return txn_aisle_cross_cate_ctrl

def get_txn_ctrl_store_feature_pre(cmp: CampaignEval,
                               prd_scope_df: SparkDataFrame):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    matched_ctrl_store_id = \
        (cmp.matched_store
        .select("ctrl_store_id")
        .drop_duplicates()
        .withColumnRenamed("ctrl_store_id", "store_id")
        )

    txn_ctrl_store_feature = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(matched_ctrl_store_id, "store_id")
         .where(F.col(period_wk_col_nm).isin(["pre"]))
         .join(prd_scope_df, 'upc_id')  
        )

    return txn_ctrl_store_feature

def get_txn_ctrl_store_total_pre(cmp: CampaignEval):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    matched_ctrl_store_id = \
        (cmp.matched_store
        .select("ctrl_store_id")
        .drop_duplicates()
        .withColumnRenamed("ctrl_store_id", "store_id")
        )

    txn_ctrl_store = \
        (cmp.txn
         .where(F.col("offline_online_other_channel")=="OFFLINE")
         .join(matched_ctrl_store_id, "store_id")
         .where(F.col(period_wk_col_nm).isin(["pre"]))
        )

    return txn_ctrl_store

def get_bask_asso_ctrl_pre(cmp: CampaignEval,
                       prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_ctrl_store_aisle_cross_cate_pre(cmp)
    feat_txn = get_txn_ctrl_store_feature_pre(cmp, prd_scope_df)

    bask_asso = (aisle_txn
                 .select("transaction_uid").drop_duplicates()
                 .join(feat_txn.select("transaction_uid").drop_duplicates(), 
                       "transaction_uid", "inner")
                 .select("transaction_uid")
                 ).drop_duplicates()

    return bask_asso

def asso_size_ctrl_pre(cmp: CampaignEval,
                       prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_ctrl_store_aisle_cross_cate_pre(cmp)
    feat_txn = get_txn_ctrl_store_feature_pre(cmp, prd_scope_df)
    bask_asso = get_bask_asso_ctrl_pre(cmp, prd_scope_df)
    total_txn = get_txn_ctrl_store_total_pre(cmp)

    size_feat_in_asso = \
        (feat_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        .withColumn("bask_type", F.lit("feat_in_asso"))
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
        )

    size_aisle_in_asso = \
        (aisle_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("aisle_in_asso"))
        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
        )
    total_store = \
        (total_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("total"))
        )
           
    total_store_visits = total_store.select("visits").collect()[0][0]

    combine = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle, total_store])
    combine_add_col = (combine.withColumn("period", F.lit("pre"))
                       .withColumn("store_type", F.lit("ctrl"))
                       .withColumn("visit_pen", F.col("visits")/total_store_visits)

    )

    return combine_add_col

#---- Visit uplift

def get_asso_kpi(cmp: CampaignEval,
                 prd_scope_df: SparkDataFrame):
    """
    """
    test_dur = asso_size_target_dur(cmp, prd_scope_df)
    test_pre = asso_size_target_pre(cmp, prd_scope_df)
    ctrl_dur = asso_size_ctrl_dur(cmp, prd_scope_df)
    ctrl_pre = asso_size_ctrl_pre(cmp, prd_scope_df)
    
    combine = reduce(union_frame, [test_dur, test_pre, ctrl_dur, ctrl_pre])
    lift = (combine
            .groupBy("store_type", "period")
            .pivot("bask_type")
            .agg(F.first("visit_pen"))
            .withColumn("lift", F.col("feat_is_asso") / (F.col("feat") * F.col("aisle")))
    )
    
    growth = (lift
            .groupBy("store_type")
            .pivot("period")
            .agg(F.first("lift"))
            .withColumn("lift_growth", F.col("dur") - F.col("pre"))
    )
    
    ctrl_factor = growth.where(F.col("store_type").isin(["ctrl"])).select("lift_growth").collect()[0][0]
    
    uplift = (growth
              .withColumn("ctrl_factor", F.lit(ctrl_factor))
              .withColumn("uplift", F.col("lift_growth") - F.col("ctrl_factor"))
    )
    
    return uplift

    
    
    
