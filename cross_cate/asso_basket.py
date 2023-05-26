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

def get_txn_aisle_cross_cate(cmp: CampaignEval):
    """
    """
    exposed.create_txn_offline_x_aisle_target_store(cmp)
    txn_aisle_cross_cate = \
        (cmp.txn_offline_x_aisle_target_store
         .where(F.col("aisle_scope").isin(["cross_cate"]))
         )
    return txn_aisle_cross_cate

def get_txn_target_store_feature(cmp: CampaignEval,
                                  prd_scope_df: SparkDataFrame):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    txn_target_store_feature = \
        (cmp.txn
         .join(cmp.target_store.select("store_id").drop_duplicates(), "store_id")
         .where(F.col(period_wk_col_nm).isin(["cmp"]))
         .join(prd_scope_df, 'upc_id')  
        )
    return txn_target_store_feature

def get_bask_asso(cmp: CampaignEval,
                  prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_aisle_cross_cate(cmp)
    feat_txn = get_txn_target_store_feature(cmp, prd_scope_df)

    bask_asso = (aisle_txn
                 .select("transaction_uid").drop_duplicates()
                 .join(feat_txn.select("transaction_uid").drop_duplicates(), 
                       "transaction_uid", "inner")
                 .select("transaction_uid")
                 ).drop_duplicates()

    return bask_asso

def asso_score(cmp: CampaignEval,
               prd_scope_df: SparkDataFrame):
    
    aisle_txn = get_txn_aisle_cross_cate(cmp)
    feat_txn = get_txn_target_store_feature(cmp, prd_scope_df)
    bask_asso = get_bask_asso(cmp, prd_scope_df)

    n_bask_aisle = aisle_txn.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_feat = feat_txn.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_asso = bask_asso.agg(F.count_distinct("transaction_uid")).collect()[0][0]

    asso_by_aisle = n_bask_asso/n_bask_aisle
    asso_by_feat = n_bask_asso/n_bask_feat

    score_df = cmp.spark.createDataFrame([("test","dur", n_bask_aisle, n_bask_feat, n_bask_asso, asso_by_aisle, asso_by_feat)],
                                     ["store_type","period", "n_bask_aisle", "n_bask_feat", "n_bask_asso", "asso_by_aisle", "asso_by_feat"])

    return score_df

def asso_size(cmp: CampaignEval,
              prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_aisle_cross_cate(cmp)
    feat_txn = get_txn_target_store_feature(cmp, prd_scope_df)
    bask_asso = get_bask_asso(cmp, prd_scope_df)

    size_feat_in_asso = \
        (feat_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        .withColumn("bask_type", F.lit("feat_in_asso"))
        .withColumn("period", F.lit("dur"))
        .withColumn("store_type", F.lit("test"))
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
         .withColumn("period", F.lit("dur"))
         .withColumn("store_type", F.lit("test"))
        )

    size_aisle_in_asso = \
        (aisle_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("aisle_in_asso"))
         .withColumn("period", F.lit("dur"))
         .withColumn("store_type", F.lit("test"))
        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
         .withColumn("period", F.lit("dur"))
         .withColumn("store_type", F.lit("test"))
        )

    combine_size = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle])

    return combine_size

def get_txn_aisle_cross_cate_pre(cmp: CampaignEval):
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
         .join(cmp.target_store.select("store_id").drop_duplicates(), "store_id")
         .where(F.col(period_wk_col_nm).isin(["pre"]))
         .join(prd_scope_df, 'upc_id')  
        )
    return txn_target_store_feature_pre

def get_bask_asso_pre(cmp: CampaignEval,
                  prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_aisle_cross_cate_pre(cmp)
    feat_txn = get_txn_target_store_feature_pre(cmp, prd_scope_df)

    bask_asso = (aisle_txn
                 .select("transaction_uid").drop_duplicates()
                 .join(feat_txn.select("transaction_uid").drop_duplicates(), 
                       "transaction_uid", "inner")
                 .select("transaction_uid")
                 ).drop_duplicates()

    return bask_asso

def asso_score_pre(cmp: CampaignEval,
               prd_scope_df: SparkDataFrame):
    
    aisle_txn = get_txn_aisle_cross_cate_pre(cmp)
    feat_txn = get_txn_target_store_feature_pre(cmp, prd_scope_df)
    bask_asso = get_bask_asso_pre(cmp, prd_scope_df)

    n_bask_aisle = aisle_txn.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_feat = feat_txn.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_asso = bask_asso.agg(F.count_distinct("transaction_uid")).collect()[0][0]

    asso_by_aisle = n_bask_asso/n_bask_aisle
    asso_by_feat = n_bask_asso/n_bask_feat

    score_df = cmp.spark.createDataFrame([("test", "pre", n_bask_aisle, n_bask_feat, n_bask_asso, asso_by_aisle, asso_by_feat)],
                                     ["store_type","period", "n_bask_aisle", "n_bask_feat", "n_bask_asso", "asso_by_aisle", "asso_by_feat"])

    return score_df

def asso_size_pre(cmp: CampaignEval,
                  prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_aisle_cross_cate_pre(cmp)
    feat_txn = get_txn_target_store_feature_pre(cmp, prd_scope_df)
    bask_asso = get_bask_asso_pre(cmp, prd_scope_df)

    size_feat_in_asso = \
        (feat_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        .withColumn("bask_type", F.lit("feat_in_asso"))
        .withColumn("period", F.lit("pre"))
        .withColumn("store_type", F.lit("test"))

        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
         .withColumn("period", F.lit("pre"))
         .withColumn("store_type", F.lit("test"))

        )

    size_aisle_in_asso = \
        (aisle_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("aisle_in_asso"))
         .withColumn("period", F.lit("pre"))
         .withColumn("store_type", F.lit("test"))

        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
         .withColumn("period", F.lit("pre"))
         .withColumn("store_type", F.lit("test"))

        )

    combine_size = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle])

    return combine_size

def get_txn_aisle_cross_cate_ctrl(cmp: CampaignEval):
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
         .where(F.col(period_wk_col_nm).isin(["dur"]))
         .join(cross_cate_ctrl_store, ["store_id", "upc_id"])
         )
    
    return txn_aisle_cross_cate_ctrl

def get_txn_ctrl_store_feature(cmp: CampaignEval,
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
         .join(matched_ctrl_store_id, "store_id")
         .where(F.col(period_wk_col_nm).isin(["cmp"]))
         .join(prd_scope_df, 'upc_id')  
        )

    return txn_ctrl_store_feature

def get_bask_asso_ctrl(cmp: CampaignEval,
                       prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_aisle_cross_cate_ctrl(cmp)
    feat_txn = get_txn_ctrl_store_feature(cmp, prd_scope_df)

    bask_asso = (aisle_txn
                 .select("transaction_uid").drop_duplicates()
                 .join(feat_txn.select("transaction_uid").drop_duplicates(), 
                       "transaction_uid", "inner")
                 .select("transaction_uid")
                 ).drop_duplicates()

    return bask_asso

def asso_score_ctrl(cmp: CampaignEval,
                    prd_scope_df: SparkDataFrame):
    
    aisle_txn = get_txn_aisle_cross_cate_ctrl(cmp)
    feat_txn = get_txn_ctrl_store_feature(cmp, prd_scope_df)
    bask_asso = get_bask_asso_ctrl(cmp, prd_scope_df)

    n_bask_aisle = aisle_txn.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_feat = feat_txn.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_asso = bask_asso.agg(F.count_distinct("transaction_uid")).collect()[0][0]

    asso_by_aisle = n_bask_asso/n_bask_aisle
    asso_by_feat = n_bask_asso/n_bask_feat

    score_df = cmp.spark.createDataFrame([("ctrl","dur", n_bask_aisle, n_bask_feat, n_bask_asso, asso_by_aisle, asso_by_feat)],
                                     ["store_type","period", "n_bask_aisle", "n_bask_feat", "n_bask_asso", "asso_by_aisle", "asso_by_feat"])

    return score_df

def asso_size_ctrl(cmp: CampaignEval,
              prd_scope_df: SparkDataFrame):
    """
    """
    aisle_txn = get_txn_aisle_cross_cate(cmp)
    feat_txn = get_txn_target_store_feature(cmp, prd_scope_df)
    bask_asso = get_bask_asso(cmp, prd_scope_df)

    size_feat_in_asso = \
        (feat_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        .withColumn("bask_type", F.lit("feat_in_asso"))
        .withColumn("period", F.lit("dur"))
        .withColumn("store_type", F.lit("ctrl"))
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
         .withColumn("period", F.lit("dur"))
         .withColumn("store_type", F.lit("ctrl"))
        )

    size_aisle_in_asso = \
        (aisle_txn
         .join(bask_asso, "transaction_uid")
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("aisle_in_asso"))
         .withColumn("period", F.lit("dur"))
         .withColumn("store_type", F.lit("ctrl"))
        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
         .withColumn("period", F.lit("dur"))
         .withColumn("store_type", F.lit("ctrl"))
        )

    combine_size = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle])

    return combine_size