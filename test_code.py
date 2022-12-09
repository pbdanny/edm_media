# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

sys.path.append(os.path.abspath("/Workspace/Repos/thanakrit.boonquarmdee@lotuss.com/edm_util"))
from edm_helper import get_lag_wk_id, to_pandas

# COMMAND ----------

resrv_store_class = "13_77_166_1"
wk_id_col_nm = "week_id"

txn = spark.table("tdm_seg.media_campaign_eval_txn_data_2022_0592_m01m")
feat_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/upc_list_2022_0592_M01M.csv", header=True, inferSchema=True).withColumnRenamed("feature", "upc_id")
test_store_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/01_hde/00_cmp_inputs/inputs_files/target_store_2022_0592_M01M.csv", header=True, inferSchema=True)

resrv_store_sf = spark.read.csv("dbfs:/FileStore/media/campaign_eval/00_std_inputs/reserved_store_HDE_20211031_master_withcode.csv", header=True, inferSchema=True).where(F.col("class_code")==resrv_store_class).select("store_id")

# COMMAND ----------

def _get_std_score(txn: SparkDataFrame,
                   wk_id_col_nm: str):
        """Calculate weekly kpi by store_id
        """
        from pyspark.sql.types import StringType
        
        def __get_std(df):
            
            from sklearn.preprocessing import StandardScaler, MinMaxScaler
            
            scalar = MinMaxScaler() # StandardScaler()
            
            scaled = scalar.fit_transform(df)
            scaled_df = pd.DataFrame(data=scaled, index=df.index, columns=df.columns)
            
            return scaled_df
        
        txn = txn.withColumn("store_id", F.col("store_id").cast(StringType()))
        
        sales = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.sum('net_spend_amt').alias('sales')).fillna(0)
        custs = txn.groupBy("store_id").pivot(wk_id_col_nm).agg(F.count_distinct('household_id').alias('custs')).fillna(0)
        
        sales_df = to_pandas(sales).astype({'store_id':str}).set_index("store_id")
        custs_df = to_pandas(custs).astype({'store_id':str}).set_index("store_id")
        
        sales_scaled_df = __get_std(sales_df)
        custs_scaled_df = __get_std(custs_df)
        
        sales_unpv_df = sales_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_sales", var_name="week_id")
        custs_unpv_df = custs_scaled_df.reset_index().melt(id_vars="store_id", value_name="std_custs", var_name="week_id")        
        comb_df = pd.merge(sales_unpv_df, custs_unpv_df, how="outer", on=["store_id", "week_id"])
        comb_df["comp_score"] = (comb_df["std_sales"] + comb_df["std_custs"])/2
        
        return comb_df

# COMMAND ----------

cr = _get_std_score(txn.join(feat_sf, "upc_id", "leftsemi"), wk_id_col_nm)

# COMMAND ----------

cr_pv = cr.pivot(index="store_id", columns="week_id", values="comp_score")

# COMMAND ----------

cr_pv

# COMMAND ----------

test_pv = cr_pv[cr_pv.index.isin(["1101", "1102", "1104", "1105"])]
ctrl_pv = cr_pv[cr_pv.index.isin(["1110", "1111"])]

# COMMAND ----------

def _get_pair_min_dist(test: PandasDataFrame,
                       ctrl: PandasDataFrame,
                       dist_nm: str):
    """From test , ctrl calculate paired distance, keep lowest
    """
    from sklearn.metrics import pairwise_distances

    data = pairwise_distances(test, ctrl, metric=dist_nm)
    paired = pd.DataFrame(data=data, index=test.index, columns=ctrl.index)    
    paired.index = paired.index.set_names("test_store_id")
    paired.columns = paired.columns.set_names("ctrl_store_id")
    paired_nm = paired.unstack().reset_index()
    paired_nm.columns = ["ctrl_store_id", "test_store_id", dist_nm]
    min_paired = paired_nm.sort_values(["test_store_id", dist_nm], ascending=True).groupby(["test_store_id"]).head(1)

    return min_paired

# COMMAND ----------

e = _get_pair_min_dist(test_pv, ctrl_pv, dist_nm="cosine")

# COMMAND ----------

e

# COMMAND ----------

c = _get_pair_min_score(test_pv, ctrl_pv, dist="cos_dist")

# COMMAND ----------

c

# COMMAND ----------

from sklearn.metrics.pairwise import paired_euclidean_distances, euclidean_distances

# COMMAND ----------

from sklearn.metrics.pairwise import cosine_distances

# COMMAND ----------

test_pv

# COMMAND ----------

ctrl_pv

# COMMAND ----------

x = euclidean_distances(test_pv, ctrl_pv)
x_df = pd.DataFrame(data = x, index=test_pv.index, columns=ctrl_pv.index)

# COMMAND ----------

x_df

# COMMAND ----------

from sklearn.metrics import pairwise_distances

pairwise_distances(test_pv, ctrl_pv, metric="")

# COMMAND ----------

x = cosine_distances(test_pv, ctrl_pv)
x_df = pd.DataFrame(data = x, index=test_pv.index, columns=ctrl_pv.index)

# COMMAND ----------

x_df.index = x_df.index.set_names("test_store_id")
x_df.columns = x_df.columns.set_names("ctrl_store_id")

# COMMAND ----------

x_df_pair = x_df.unstack().reset_index()
x_df_pair.columns = ["ctrl_store_id", "test_store_id", "score"]

# COMMAND ----------

x_df_pair

# COMMAND ----------

x_df_pair.sort_values(["test_store_id", "score"], ascending=True).groupby(["test_store_id"]).head(1)

# COMMAND ----------

paired_euclidean_distances(test_pv, test_pv)

# COMMAND ----------



for i, test_row in test_pv.iterrows():
    for j, ctrl_row in ctrl_pv.iterrows():
        print(i, "-", j)
        print(test_row[1:])
        print(ctrl_row[1:])

# COMMAND ----------

ctrl_pv.shape

# COMMAND ----------

test_pv

# COMMAND ----------

for i,  in cr_pv.iterrows():
    print(i)
    print(row[1:])

# COMMAND ----------



# COMMAND ----------

    from sklearn.metrics.pairwise import cosine_similarity

# COMMAND ----------

i1 = cr_pv[cr_pv["store_id"].isin(["1101", "1102"])].iloc[0, 1:]
i2 = cr_pv[cr_pv["store_id"].isin(["1101", "1102"])].iloc[1, 1:]

# COMMAND ----------

np.var(np.abs(i1 - i2))

# COMMAND ----------

from statistics import variance
variance(np.abs(i1-i2))

# COMMAND ----------

from scipy.spatial import distance

distance.euclidean(i1, i2)

# COMMAND ----------

from sklearn.metrics.pairwise import cosine_similarity, cosine_distances
cosine_similarity(i1, i2)

# COMMAND ----------

distance.cosine(i1, i2)

# COMMAND ----------

cosine_distances(i1.to_numpy().reshape(1, -1), i2.to_numpy().reshape(1, -1))

# COMMAND ----------

cosine_similarity(i1.to_numpy().reshape(1, -1), i2.to_numpy().reshape(1, -1))

# COMMAND ----------

i1.to_numpy().reshape(1, -1)

# COMMAND ----------

import seaborn as sns
sns.kdeplot(data=cr, x="comp_score", hue="week_id")
# cr[["comp_score"]].plot(kind="kde")

# COMMAND ----------

cr

# COMMAND ----------

cr.pivot(columns="week_id", values="comp_score").reset_index()

# COMMAND ----------

cr_pv = cr.pivot(index="store_id", columns="week_id", values="comp_score").reset_index()

# COMMAND ----------

cr_pv

# COMMAND ----------

import numpy as np
from sklearn.metrics import pairwise_distances
from sklearn.metrics.pairwise import paired_euclidean_distances
X = pd.DataFrame(np.array([[2, 3]]), columns=["x", "y"])
Y = pd.DataFrame(np.array([[1, 0]]), columns=["x", "y"])

# COMMAND ----------

paired_euclidean_distances(X, Y)

# COMMAND ----------

X = pd.DataFrame({"store_id":[1, 2, 3],
                 "wk1":[1.2, 3.3, 4.3],
                 "wk2":[2.2, 1.3, 2.3]})

# COMMAND ----------

pd.merge(cr_pv, cr_pv, how="cross")

# COMMAND ----------


