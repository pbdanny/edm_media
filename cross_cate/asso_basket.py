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

def get_bask_aisle_cross_cate(cmp: CampaignEval):
    """
    """
    exposed.create_txn_offline_x_aisle_target_store(cmp)
    bask_aisle_cross_cate = \
        (cmp.txn_offline_x_aisle_target_store
         .where(F.col("aisle_scope").isin(["cross_cate"]))
         )
    return bask_aisle_cross_cate

def get_bask_target_store_feature(cmp: CampaignEval,
                                  prd_scope_df: SparkDataFrame):
    """
    """
    period_wk_col_nm = period_cal.get_period_wk_col_nm(cmp)

    bask_target_store_feature = \
        (cmp.txn
         .join(cmp.target_store.select("store_id").drop_duplicates(), "store_id")
         .where(F.col(period_wk_col_nm).isin(["cmp"]))
         .join(prd_scope_df, 'upc_id')  
        )
    return bask_target_store_feature

def get_bask_asso(cmp: CampaignEval,
                  prd_scope_df: SparkDataFrame):
    """
    """
    aisle_bask = get_bask_aisle_cross_cate(cmp)
    feat_bask = get_bask_target_store_feature(cmp, prd_scope_df)

    bask_asso = aisle_bask.join(feat_bask, "transaction_uid", "inner").select("transaction_uid").drop_duplicates()

    return bask_asso

def asso_score(cmp: CampaignEval,
               prd_scope_df: SparkDataFrame):
    
    aisle_bask = get_bask_aisle_cross_cate(cmp)
    feat_bask = get_bask_target_store_feature(cmp, prd_scope_df)
    bask_asso = get_bask_asso(cmp, prd_scope_df)

    n_bask_aisle = aisle_bask.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_feat = feat_bask.agg(F.count_distinct("transaction_uid")).collect()[0][0]
    n_bask_asso = bask_asso.agg(F.count_distinct("transaction_uid")).collect()[0][0]

    asso_by_aisle = n_bask_asso/n_bask_aisle
    asso_by_feat = n_bask_asso/n_bask_feat

    return asso_by_aisle, asso_by_feat

def asso_size(cmp: CampaignEval,
              prd_scope_df: SparkDataFrame):
    """
    """
    aisle_bask = get_bask_aisle_cross_cate(cmp)
    feat_bask = get_bask_target_store_feature(cmp, prd_scope_df)
    bask_asso = get_bask_asso(cmp, prd_scope_df)

    size_feat_in_asso = \
        (feat_bask
         .join(bask_asso, "transaction_uid")
         .withColumn("bask_type", F.lit("feat_in_asso"))
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        )
    size_feat = \
        (feat_bask
         .withColumn("bask_type", F.lit("feat"))
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        )

    size_aisle_in_asso = \
        (aisle_bask
         .join(bask_asso, "transaction_uid")
         .withColumn("bask_type", F.lit("aisle_in_asso"))
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        )
    size_aisle = \
        (aisle_bask
         .withColumn("bask_type", F.lit("aisle"))
         .agg(F.sum("net_spend_amt").alias("sales"),
              F.count_distinct("transaction_uid").alias("visits"),
              (F.sum("net_spend_amt")/F.count_distinct("transaction_uid")).alias("spv")
         )
        )

    combine_size = reduce(union_frame, [size_feat_in_asso, size_feat, size_aisle_in_asso, size_aisle])

    return combine_size