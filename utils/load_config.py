#!/usr/bin/env python.
# -*- coding: utf-8 -*-

"""
Author: thanakrit.boo@ascendcorp.com
Project: Media Evaluation
Created: 05-May-2022
Python-Version: 3.8
---
Utils function for Media Evaluation solution
"""
from typing import List

from pyspark.sql import DataFrame as SparkDataFrame

def get_period_wk_col_nm(
        wk_type: str
    ) -> str:
    """Column name for flagging period
    List of value in colum : ppp = prior, pre, cmp = dur
    """
    if wk_type in ["promo_week", "promo_wk"]:
        period_wk_col_nm = "period_promo_wk"
    elif wk_type in ["promozone"]:
        period_wk_col_nm = "period_promo_mv_wk"
    else:
        period_wk_col_nm = "period_fis_wk"
    return period_wk_col_nm

def get_test_store_sf(
        test_store_sf: SparkDataFrame,
        cp_start_date: str,
        cp_end_date: str
    ) -> SparkDataFrame:
    """From input target store files, 
    fill c_start, c_end based on cp_start_date, cp_end_date
    """
    filled_test_store_sf = \
        (test_store_sf
        .fillna(str(cp_start_date), subset='c_start')
        .fillna(str(cp_end_date), subset='c_end')
        )
    return filled_test_store_sf