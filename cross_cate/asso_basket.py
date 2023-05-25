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

    score_df = spark.createDataFrame([("dur", n_bask_aisle, n_bask_feat, n_bask_asso, asso_by_aisle, asso_by_feat)],
                                     ["period", "n_bask_aisle", "n_bask_feat", "n_bask_asso", "asso_by_aisle", "asso_by_feat"])

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
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
         .withColumn("period", F.lit("dur"))
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
        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
         .withColumn("period", F.lit("dur"))
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

    score_df = spark.createDataFrame([("pre", n_bask_aisle, n_bask_feat, n_bask_asso, asso_by_aisle, asso_by_feat)],
                                     ["period", "n_bask_aisle", "n_bask_feat", "n_bask_asso", "asso_by_aisle", "asso_by_feat"])

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
        )
    size_feat = \
        (feat_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
         .withColumn("bask_type", F.lit("feat"))
         .withColumn("period", F.lit("pre"))
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
        )
    size_aisle = \
        (aisle_txn
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
            )
         .withColumn("bask_type", F.lit("aisle"))
         .withColumn("period", F.lit("pre"))
        )

    combine_size = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle])

    return combine_size