from typing import List
from copy import deepcopy
from datetime import datetime, timedelta
import functools

import os
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from numpy import ndarray as numpyNDArray

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

from utils.DBPath import DBPath
from utils.campaign_config import CampaignEval

spark = SparkSession.builder.appName("clear_temp").getOrCreate()

def clear_attr_and_temp_tbl(cmp: CampaignEval):
    """Clear attribute from campaing objeect & delete temp table (if any)
    """
    # clear aisle_target_store_conf
    if hasattr(cmp, "aisle_target_store_conf"):
        delattr(cmp, "aisle_target_store_conf")
    tbl_nm = f"tdm_dev.th_lotuss_media_eval_aisle_target_store_conf_{cmp.params['cmp_id']}_temp"
    print(f"Drop temp table (if exist) {tbl_nm}")
    spark.sql(f"DROP TABLE IF EXISTS {tbl_nm}")
    
    # clear cust_purchased_exposure_count
    tbl_nm_pattern = f"th_lotuss_media_eval_cust_purchased_exposure_count_{cmp.params['cmp_id'].lower()}_lv*"
    tables = spark.sql(f"SHOW TABLES IN tdm_dev LIKE '{tbl_nm_pattern}'")
    for row in tables.collect():
        print(f"Drop temp table (if exist) tdm_dev.{row[1]}")
        spark.sql(f"DROP TABLE IF EXISTS tdm_dev.{row[1]}")
