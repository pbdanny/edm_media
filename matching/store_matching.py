import pprint
from ast import literal_eval
from typing import List, Union
from datetime import datetime, timedelta
import sys
import os

import numpy as np
from pandas import DataFrame as PandasDataFrame
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

sys.path.append(os.path.abspath(
    "/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))

import edm_helper

from utils.DBPath import DBPath
from utils import period_cal
from utils.campaign_config import CampaignEval, CampaignEvalO3
from exposure.exposed import create_txn_offline_x_aisle_target_store

from utils import period_cal

#---- Developing
def forward_compatible_stored_matching_schema(cmp):
    """Perform forward compatibility adjustments from matching store saved from in version 1.
    
    Args:
        cmp (CampaignEval): The CampaignEval object containing the stored transactions and necessary information.
        
    Returns:
        None
    """        
    if "store_id" in cmp.matched_store.columns:
        cmp.matched_store = cmp.matched_store.drop("test_store_id").withColumnRenamed("store_id", "test_store_id")
        
    if "ctr_store_cos" in cmp.matched_store.columns:
        cmp.matched_store = cmp.matched_store.drop("ctrl_store_id").withColumnRenamed("ctr_store_cos", "ctrl_store_id")
    return

def get_backward_compatible_stored_matching_schema(cmp):
    """Perform backward compatibility adjustments to the matching store version 1.
    
    Args:
        cmp (CampaignEval): The CampaignEval object containing the stored transactions and necessary information.
        
    Returns:
        None
    """        
    if "test_store_id" in cmp.matched_store.columns:
        back_matched_store = cmp.matched_store.drop("store_id").withColumnRenamed("test_store_id", "store_id")
        
    if "ctrl_store_id" in cmp.matched_store.columns:
        back_matched_store = back_matched_store.drop("ctr_store_cos").withColumnRenamed( "ctrl_store_id", "ctr_store_cos")
    
    return back_matched_store

def get_store_matching_across_region(cmp: Union[CampaignEval, CampaignEvalO3],
                                     matching_methodology: str = 'cosine_distance',
                                     bad_match_threshold: float = 2.5):
    """
    This function performs store matching across regions based on a specified methodology.

    Parameters:
    -----
    - cmp (Union[CampaignEval, CampaignEvalO3]): The campaign object containing relevant data for store matching.
    - matching_methodology (str, default='cosine_distance'): The method used to calculate distance between stores. Can be 'varience', 'euclidean', or 'cosine_distance'.
    - bad_match_threshold (float, default=2.5): The threshold value for identifying outlier stores (bad matches) using the Median Absolute Deviation (MAD) method.

    Returns:
    ----
    - None

    This function performs the following steps:

    1. **Input Validation:**
       - Checks if the 'matched_store' attribute already exists in the campaign object. If so, it exits without further processing.
       - Attempts to load a previously saved 'matched_store' DataFrame from the campaign's output path. If successful, it updates the campaign object and exits.

    2. **Data Preparation:**
       - Extracts relevant data from the campaign object, including transaction data (txn), week type (wk_type), pre-period start and end week IDs (pre_st_wk, pre_en_wk), feature DataFrames for SKU (feat_sku), brand (feat_brand_sku), and subclass (feat_subclass_sku), target and control store DataFrames (test_store_sf, control_store), and the column name representing the week ID (wk_id_col_nm) based on the campaign type.
       - Defines helper functions for:
           - Calculating the minimum number of sales weeks for target and control stores (_get_min_wk_sales).
           - Calculating the weekly average composite score (sales and customer count) for each store (_get_comp_score).
           - Calculating pairwise distances between stores using different distance metrics (_get_pair_min_dist, _get_pair_min_dist_func).
           - Identifying outlier stores (bad matches) using the MAD method (_flag_outlier_mad).
           - Plotting comparisons between matched store pairs (__plt_pair, optional).

    3. **Main Logic:**
       - Prints a message indicating the start of the store matching process.
       - Identifies the week ID column name and prints a message specifying the column used.
       - Limits matching to the "OFFLINE" channel.
       - Finds the minimum required sales weeks for target and control stores to be included in the matching process.
       - Selects the appropriate matching level (SKU, brand, or subclass) based on the minimum sales weeks.
       - Stores the matching level, minimum sales weeks for target and control stores in the campaign object's parameters.
       - Prints messages indicating the chosen matching level and methodology.

    4. **Composite Score Calculation:**
       - Calculates the weekly average composite score (sales and customer count) for each store involved in the matching process (including target and control stores).

    5. **Store Matching:**
       - Creates a Pandas DataFrame representation of the store composite scores for easier manipulation.
       - Iterates through regions identified from the store data.
          - For each region:
              - Extracts store IDs, regions, and types (test or control) for stores in that region.
              - Filters the composite score DataFrame to include only stores in the current region.
              - Converts the composite score DataFrame to a matrix with store IDs as index and week IDs as columns.
              - Calculates pairwise distances between test and control stores using the specified matching methodology.
              - Identifies outlier (bad match) store pairs using the MAD method and a predefined threshold.
              - Prints information about the identified bad matches, including details about the store pairs and their outlier scores.

    6. **Output:**
       - Creates a list to store control stores identified through the matching process.
       - Creates a Pandas DataFrame containing details of the matched store pairs (test store ID, control store ID, test store region, control store region).
       - Updates the campaign object with the list of matched control stores and the DataFrame containing matched store pairs.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    from sklearn.preprocessing import StandardScaler

    from scipy.spatial import distance
    import statistics as stats
    from sklearn.metrics.pairwise import cosine_similarity

    if hasattr(cmp, "matching_df"):
        print("Campaign object already have attribute 'matched_store'")
        cmp.ctr_store_list = cmp.matching_df["ctr_store_cos"].unique().tolist()
        return cmp.ctr_store_list, cmp.matching_df
    
    try:
        cmp.matching_df = pd.read_csv((cmp.output_path/"output"/"store_matching.csv").file_api())
        # forward_compatible_stored_matching_schema(cmp)
        cmp.ctr_store_list = cmp.matching_df["ctr_store_cos"].unique().tolist()
        print(f"Load 'matched_store' from {(cmp.output_path/'output'/'store_matching.csv').file_api()}" )
        return cmp.ctr_store_list, cmp.matching_df
    
    except Exception as e:
        print(e)
        pass

    txn = cmp.txn
    wk_type = cmp.wk_type
    pre_st_wk = cmp.pre_st_wk
    pre_en_wk = cmp.pre_en_wk
    feat_sf = cmp.feat_sku
    brand_df = cmp.feat_brand_sku
    sclass_df = cmp.feat_subclass_sku
    test_store_sf = cmp.target_store
    control_store = cmp.control_store
       
    wk_id_col_nm = period_cal.get_wk_id_col_nm(cmp)
    
    cmp.params["matching_level"] = "store_lv"
    cmp.params["matching_methodology"] = matching_methodology
    cmp.params["bad_match_threshold"] = bad_match_threshold
        
    #---- Helper fn

    def _get_min_wk_sales(prod_scope_df: SparkDataFrame):
        """Count number of week sales by store, return the smallest number of target store, control store
        """
        # Min sales week test store
        txn_match_trg  = (txn
                          .join(test_store_sf.drop("store_region_orig", "store_region"), "store_id", "inner")
                          .join(prod_scope_df, "upc_id", "leftsemi")
                          .where(F.col(wk_id_col_nm).between(pre_st_wk, pre_en_wk))
                          .where(F.col("offline_online_other_channel") == 'OFFLINE')
                          .select(txn['*'],
                                F.col("store_region").alias('store_region_new'),
                                F.lit('test').alias('store_type'),
                                F.col("mech_name").alias('store_mech_set'))
                          )
        trg_wk_cnt_df = txn_match_trg.groupBy(F.col("store_id")).agg(F.count_distinct(F.col(wk_id_col_nm)).alias('wk_sales'))
        trg_min_wk = trg_wk_cnt_df.agg(F.min(trg_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0][0]

        # Min sale week ctrl store
        txn_match_ctl = (txn
                         .join(control_store.drop("store_region_orig", "store_region"), "store_id", 'inner')
                         .join(prod_scope_df, "upc_id", "leftsemi")
                         .where(F.col(wk_id_col_nm).between(pre_st_wk, pre_en_wk))
                         .where(F.col("offline_online_other_channel") == 'OFFLINE')
                         .select(txn['*'],
                                 F.col("store_region").alias('store_region_new'),
                                 F.lit('ctrl').alias('store_type'),
                                 F.lit('No Media').alias('store_mech_set'))
                         )

        ctl_wk_cnt_df = txn_match_ctl.groupBy("store_id").agg(F.count_distinct(F.col(wk_id_col_nm)).alias('wk_sales'))
        ctl_min_wk = ctl_wk_cnt_df.agg(F.min(ctl_wk_cnt_df.wk_sales).alias('min_wk_sales')).collect()[0][0]

        return int(trg_min_wk), txn_match_trg, int(ctl_min_wk), txn_match_ctl

    def _get_comp_score(txn: SparkDataFrame,
                        wk_id_col_nm: str):
        """Calculate weekly avg composite score by store_id
        Composite score = (normalized sales + normalize customer) / 2
        Normalize = MinMaxScalar, normalized each week separately

        Parameter
        ----
        txn : SparkDataFrame
            transaction of product level (feature sku, feature brand, subclass) for score calculation.

        wk_id_col_nm : str
            Name of week column (week_id / promo_week_id) to calculate weekly kpi.

        """
        def __get_std(df: PandasDataFrame) -> PandasDataFrame:
            from sklearn.preprocessing import StandardScaler, MinMaxScaler

            scalar = MinMaxScaler() # StandardScaler()

            scaled = scalar.fit_transform(df)
            scaled_df = pd.DataFrame(data=scaled, index=df.index, columns=df.columns)

            return scaled_df

        txn = txn.withColumn("store_id", F.col("store_id").cast(StringType()))

        sales = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
        custs = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.count_distinct('household_id').alias('custs')).fillna(0)

        sales_df = edm_helper.to_pandas(sales).astype({'store_id':str}).set_index("store_id")
        custs_df = edm_helper.to_pandas(custs).astype({'store_id':str}).set_index("store_id")

        sales_scaled_df = __get_std(sales_df)
        custs_scaled_df = __get_std(custs_df)

        sales_unpv_df = sales_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_sales", var_name="week_id")
        custs_unpv_df = custs_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_custs", var_name="week_id")
        comb_df = pd.merge(sales_unpv_df, custs_unpv_df, how="outer", on=["store_id", "week_id"])
        comb_df["comp_score"] = (comb_df["std_sales"] + comb_df["std_custs"])/2

        return comb_df

    def __var_abs_dist(x: pd.Series, y: pd.Series):
        """Custom distance function "Varience"
        """
        import numpy as np
        var_abs = np.var(np.abs(x - y))
        return var_abs

    def __get_min_pair(data: np.ndarray,
                       test: pd.DataFrame,
                       ctrl: pd.DataFrame,
                       dist_nm: str):
        """Converted paired distance ndarray, row = test store x column = control store
        into PandasDataFrame and find min paired

        Parameter
        ----
        data : np.ndarray
            Paired distance matrix test x ctrl
        test : PandasDataFrame
            PandasDataFrame of test store id
        ctrl : PandasDataFrame
            PandasDataFrame of control store id
        dist_nm : str
            Name of distance measure
        """

        paired = pd.DataFrame(data=data, index=test.index, columns=ctrl.index)
        paired.index = paired.index.set_names("test_store_id")
        paired.columns = paired.columns.set_names("ctrl_store_id")
        # Turn wide format into long format with paired of ctrl - test store id, rename column 0 -> value
        paired_nm = paired.unstack().reset_index().rename(columns={0:"value"})

        # Add distance measure into column, sort column
        paired_nm["dist_measure"] = dist_nm
        paired_nm = paired_nm.loc[:, ["test_store_id", "ctrl_store_id", "dist_measure", "value"]]
        # Find lowest score of each test paired with ctrl
        min_paired = paired_nm.sort_values(["test_store_id", "value"], ascending=True).groupby(["test_store_id"]).head(1)

        return min_paired

    def _get_pair_min_dist(test: PandasDataFrame,
                           ctrl: PandasDataFrame,
                           dist_nm: str):
        """From test , ctrl calculate paired distance, keep lowest pair
        The test , ctrl DataFrame must have 'store_id' as index

        Parameter
        ----
        dist_nm : 'euclidean' or 'cosine'
        """
        from sklearn.metrics import pairwise_distances

        data = pairwise_distances(test, ctrl, metric=dist_nm)

        return __get_min_pair(data, test, ctrl, dist_nm)

    def _get_pair_min_dist_func(test: PandasDataFrame,
                                ctrl: PandasDataFrame,
                                dist_nm: str,
                                dist_func):
        """From test , ctrl calculate paired distance of custom distance function, keep lowest pair
        The test , ctrl DataFrame must have 'store_id' as index

        Parameter
        ----
        dist_nm : str

        dist_func : object
            custom distance function
        """

        import numpy as np

        data = np.zeros( (test.shape[0], ctrl.shape[0]) )

        for i, r in test.reset_index(drop=True).iterrows():
            for j, l in ctrl.reset_index(drop=True).iterrows():
                data[i, j] = dist_func(r, l)

        return __get_min_pair(data, test, ctrl, dist_nm)

    def _flag_outlier_mad(df: PandasDataFrame,
                          outlier_score_threshold: float):
        """Flag outlier with MAD method
        Parameter
        ----
        df:
            PandasDataframe with column "value"

        outlier_score_threshold:
            threshold = 3.0

        Return
        ----
        : PandasDataFrame
            with column "flag_outlier"

        """
        NORMAL_DISTRIBUTION_CONSTANT = 1.4826

        flag = \
        (df
         .assign(median = lambda x : x["value"].median())
         .assign(abs_deviation = lambda x : np.abs(x["value"] - x["median"]) )
         .assign(mad = lambda x : NORMAL_DISTRIBUTION_CONSTANT * np.median( x["abs_deviation"] ) )
         .assign(outlier_score = lambda x : x["abs_deviation"]/x["mad"])
         .assign(flag_outlier = lambda x : np.where(x["outlier_score"]>=outlier_score_threshold, True, False))
        )

        return flag

    def __plt_pair(pair: PandasDataFrame,
                   store_comp_score: PandasDataFrame):
        """Comparison plot each pair of test-ctrl score
        """
        from matplotlib import pyplot as plt

        for i, row in pair.iterrows():
            test_score = store_comp_score[store_comp_score["store_id"]==row["test_store_id"]].loc[:,["week_id", "comp_score"]]
            ctrl_score = store_comp_score[store_comp_score["store_id"]==row["ctrl_store_id"]].loc[:,["week_id", "comp_score"]]
            fig, ax = plt.subplots()
            test_score.plot(ax=ax, x="week_id", y="comp_score", label=f'Test Store : {row["test_store_id"]}')
            ctrl_score.plot(ax=ax, x="week_id", y="comp_score", label=f'Ctrl Store : {row["ctrl_store_id"]}')
            plt.show()

        return None

    #--------------
    #---- Main ----
    #--------------

    print("-"*80)
    wk_id_col_nm = period_cal.get_wk_id_col_nm(cmp)
    print(f"Week_id based on column '{wk_id_col_nm}'")
    print('Matching performance only "OFFLINE" channel')

    pre_st_wk  = edm_helper.get_lag_wk_id(wk_id=pre_en_wk, lag_num=13, inclusive=True)

    # Find level for matching : feature sku / (feature) brand / (feature) subclass
    trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(feat_sf)

    if (trg_min_wk >= 3) & (ctl_min_wk >= 3):
        match_lvl = 'feature sku'
        txn_matching = txn_match_trg.union(txn_match_ctl)

    else:
        trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(brand_df)
        if (trg_min_wk >= 3) & (ctl_min_wk >= 3):
            match_lvl = 'brand'
            txn_matching = txn_match_trg.union(txn_match_ctl)
        else:
            trg_min_wk, txn_match_trg, ctl_min_wk, txn_match_ctl = _get_min_wk_sales(sclass_df)
            match_lvl = 'subclass'
            txn_matching = txn_match_trg.union(txn_match_ctl)
    
    # Store match level , and min week match level for target , control store
    cmp.params["match_lvl"] = match_lvl
    cmp.params["trg_min_wk"] = trg_min_wk
    cmp.params["ctrl_min_wk"] = ctl_min_wk
    print(f'This campaign will do matching at "{match_lvl.upper()}"')
    print(f'Matching method "{matching_methodology.upper()}"')
    print("-"*80)

    # Get composite score by store
    store_comp_score = _get_comp_score(txn_matching, wk_id_col_nm)
    store_comp_score_pv = store_comp_score.pivot(index="store_id", columns="week_id", values="comp_score").reset_index()

    # get store_id, store_region_new, store_type, store_mech_set
    store_type = txn_matching.select(F.col("store_id").cast(StringType()), "store_region_new", "store_type", "store_mech_set").drop_duplicates().toPandas()
    region_list = store_type["store_region_new"].dropna().unique()

    #---- New part : store_id as index
    store_comp_score_pv_id = store_comp_score.pivot(index="store_id", columns="week_id", values="comp_score")

    euc_list = []
    cos_list = []
    var_list = []

    # List of store_id in those region for test, ctrl
    test_store_id = store_type[(store_type["store_type"]=="test")]
    ctrl_store_id = store_type[(store_type["store_type"]=="ctrl")]

    # Store_id and score for test, ctrl
    test_store_score = store_comp_score_pv_id[store_comp_score_pv_id.index.isin(test_store_id["store_id"])]
    ctrl_store_score = store_comp_score_pv_id[store_comp_score_pv_id.index.isin(ctrl_store_id["store_id"])]

    pair_min_euc = _get_pair_min_dist(test=test_store_score, ctrl=ctrl_store_score, dist_nm="euclidean")
    pair_min_cos = _get_pair_min_dist(test=test_store_score, ctrl=ctrl_store_score, dist_nm="cosine")
    pair_min_var = _get_pair_min_dist_func(test=test_store_score, ctrl=ctrl_store_score, dist_func=__var_abs_dist, dist_nm="var_abs")

    euc_list.append(pair_min_euc)
    cos_list.append(pair_min_cos)
    var_list.append(pair_min_var)

    all_euc = pd.concat(euc_list)
    all_cos = pd.concat(cos_list)
    all_var = pd.concat(var_list)

    all_dist_no_region = pd.concat([all_euc, all_cos, all_var])

    # Map store region
    #str_region = txn_matching.select(F.col("store_id").cast(StringType()), "store_region_new").drop_duplicates().toPandas()
    str_region = txn_matching.select(F.col("store_id").cast(StringType()), "store_region_new", F.col("store_mech_set")).drop_duplicates().toPandas()  ### --  Add 'store_mech_set' back to dataframe  -- Pat 8 FEb 2023
    test_str_region = str_region.rename(columns={"store_id":"test_store_id", "store_region_new":"test_store_region"})
    ctrl_str_region = str_region.rename(columns={"store_id":"ctrl_store_id", "store_region_new":"ctrl_store_region"}).drop("store_mech_set", axis=1)
    all_dist = all_dist_no_region.merge(test_str_region, on="test_store_id", how="left").merge(ctrl_str_region, on="ctrl_store_id", how="left")

    # print("All distance method - matching result")
    # all_dist.display()
    # print("Summary all distance method value")
    # all_dist.groupby(["dist_measure"])["value"].agg(["count", np.mean, np.std]).reset_index().display()
    # all_dist.groupby(["dist_measure", "test_store_region"])["value"].agg(["count", np.mean, np.std]).reset_index().display()

    #---- Set outlier score threshold
    OUTLIER_SCORE_THRESHOLD = bad_match_threshold
    #---- Select control store from distance method + remove outlier
    if matching_methodology == 'varience':
        flag_outlier = _flag_outlier_mad(all_var, outlier_score_threshold=OUTLIER_SCORE_THRESHOLD)
    elif matching_methodology == 'euclidean':
        flag_outlier = _flag_outlier_mad(all_euc, outlier_score_threshold=OUTLIER_SCORE_THRESHOLD)
    elif matching_methodology == 'cosine_distance':
        flag_outlier = _flag_outlier_mad(all_cos, outlier_score_threshold=OUTLIER_SCORE_THRESHOLD)
    else:
        print('Matching methodology not in scope list : varience, euclidean, cosine_distance')
        return None

    print("-"*80)
    print(f"Paired test-ctrl before remove bad match")
    flag_outlier.display()

    print("-"*80)
    no_outlier = flag_outlier[~flag_outlier["flag_outlier"]]
    print(f"Number of paired test-ctrl after remove bad match : {no_outlier.shape[0]}")
    # no_outlier.display()

    # print("Pair plot matched store")
    # __plt_pair(no_outlier, store_comp_score=store_comp_score)

    print("-"*80)
    print(f"Outlier score threshold : {OUTLIER_SCORE_THRESHOLD}")
    print("Details of bad match pair(s)")
    outlier = flag_outlier[flag_outlier["flag_outlier"]]
    
    #if outlier.count() > 0:
    if outlier.empty :  ## Pat change to use ".empty" to check empty df -- 14 feb 2023
        print("No outlier (no bad match)")        
    else: ## have bad match
        (outlier
        .merge(test_str_region, on="test_store_id", how="left")
        .merge(ctrl_str_region, on="ctrl_store_id", how="left")
        ).display()

    # print("Pair plot bad match store")
    #__plt_pair(outlier, store_comp_score=store_comp_score)

    # Create control store list from matching result & remove bad match
    ctr_store_list = list(set([s for s in no_outlier.ctrl_store_id]))

    # Create matched test-ctrl store id & remove bad match pairs
    # Backward compatibility : rename `test_store_id` -> `store_id` and `ctrl_store_id` -> `ctr_store_var`
    matching_df = (no_outlier
                   .merge(test_str_region, on="test_store_id", how="left")
                   .merge(ctrl_str_region, on="ctrl_store_id", how="left")
                   .rename(columns={"test_store_id":"store_id", "ctrl_store_id":"ctr_store_cos"})                   
                  )

    cmp.ctr_store_list = ctr_store_list
    cmp.matching_df = matching_df
    
    return cmp.ctr_store_list, cmp.matching_df