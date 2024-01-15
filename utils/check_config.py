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

def check_target_store(cmp):
    """Base on store group name,
    - if HDE / Talad -> count check test vs total store
    - if GoFresh -> adjust 'store_region' in txn, count check
    """
    from typing import List
    from pyspark.sql import DataFrame as SparkDataFrame

    print("-" * 80)
    print("Count check store region")
    print(f"Store format defined : {cmp.store_fmt}")

    def _get_all_and_test_store(format_id_list: List):
        """Get universe store count, based on format definded
        If store region Null -> Not show in count
        """
        all_store_count_region = (
            cmp.store_dim
            .where(F.col("format_id").isin(format_id_list))
            .groupBy("store_region")
            .agg(F.count("store_id").alias(f"total_{cmp.store_fmt}"))
        )

        test_store_count_region = (
            cmp.store_dim
            .join(cmp.target_store, "store_id", "left_semi")
            .drop("store_format_name")
            .withColumn(
                "store_format_name",
                F.when(F.col("format_id").isin([1, 2, 3]), "target_hyper")
                .when(F.col("format_id").isin([4]), "target_super")
                .when(F.col("format_id").isin([5]), "target_mini_super")
                .otherwise("target_other_fmt"),
            )
            .groupBy("store_region")
            .pivot("store_format_name")
            .agg(F.count("store_id").alias(f"test_store_count"))
        )

        return all_store_count_region, test_store_count_region

    cmp.load_target_store()

    if cmp.store_fmt in ["hde", "hyper"]:
        all_store_count_region, test_store_count_region = _get_all_and_test_store(
            [1, 2, 3]
        )

    elif cmp.store_fmt in ["talad", "super"]:
        all_store_count_region, test_store_count_region = _get_all_and_test_store([4])

    elif cmp.store_fmt in ["gofresh", "mini_super"]:
        all_store_count_region, test_store_count_region = _get_all_and_test_store([5])
        
    else:
        print(f"Unknown store format group name : {cmp.store_fmt}")
        return None

    test_vs_all_store_count = all_store_count_region.join(
        test_store_count_region, "store_region", "left"
    ).orderBy("store_region")

    return test_vs_all_store_count

def get_target_control_store_dup(cmp):
    """4 Jan 2023  -- Paksirinat Chanchana - initial version

    2 input parameter as sparkdataframe (target_store_df, control_store_df)
    2 output variable

    > n_store_dup : Number of duplicate stores (expected to be zero)
    > dup_str_list: List of store_id which duplicate between 2 dataframe

    """
    trg_str_df = cmp.target_store
    ctl_str_df = cmp.control_store
    chk_store_dup_df = ctl_str_df.join(
        trg_str_df, [ctl_str_df.store_id == trg_str_df.store_id], "left_semi"
    )
    n_store_dup = (
        chk_store_dup_df.agg(F.sum(F.lit(1)).alias("n_store_dup"))
        .collect()[0]
        .n_store_dup
    )

    if n_store_dup is None:
        n_store_dup = 0

    print(
        " Number of duplicate stores between target & control = "
        + str(n_store_dup)
        + " stores \n"
    )
    print("=" * 80)

    if n_store_dup > 1:
        dup_str_list = (
            chk_store_dup_df.toPandas()["store_id"].drop_duplicates().to_list()
        )
    else:
        dup_str_list = []

    return n_store_dup, dup_str_list
