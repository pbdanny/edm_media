# Databricks notebook source
# MAGIC %run /Users/thanakrit.boonquarmdee@lotuss.com/utils/std_import

# COMMAND ----------

from scipy.stats import expon, erlang
import pandas as pd
import numpy as np

# COMMAND ----------

file_location = "dbfs:/FileStore/thanakrit/temp/to_export/purchase_cycle_dev.csv"

df = \
(spark.read.csv(file_location, inferSchema=True, header=True)
 .withColumn("date_id", F.to_date(F.col("date_id"), format="MM/dd/yy"))
 .withColumn("prev_date_id", F.to_date(F.col("prev_date_id"), format="MM/dd/yy"))
)

# COMMAND ----------

df.display()

# COMMAND ----------

def backtest_smooth_purchase_cyc_alpha(sf, test_alpha):
    """Use backtest to get error of predicted purchase cycle with exponential smoothing factor (test_alpha)
    Use latest(T=1) purchase cycle as target, use T=2, T=3 of purchase cycle as features
    
    If purchase cycle at T=3 not available, then ignore smoothing and use T=2 as prediction of target.
    
    Exponential smoothing formula
    yt = alpha*y,t-1 + (1-alpha)*y,t-2
    """
    
    alpha = test_alpha
    alpha_1 = (1-test_alpha)
    MAX_LATEST_NUM_PURCHASE = 3

    n = sf.count()
    out = (sf
           .withColumn("desc_order", F.row_number().over(Window.partitionBy("household_id").orderBy(F.col("date_id").desc_nulls_last())))
           .where(F.col("desc_order")<=MAX_LATEST_NUM_PURCHASE)
           .groupBy("household_id")
           .pivot("desc_order")
           .agg(F.sum("day_diff"))  # F.sum no effect , just pivot
           .withColumn("smth_prchs_cyc", F.when(F.col("3").isNotNull(), F.col("2")*alpha + F.col("3")*alpha_1).otherwise(F.col("2")))
           .where(F.col("smth_prchs_cyc").isNotNull())
           .withColumn("abs_error", F.abs(F.col("smth_prchs_cyc")-F.col("1")))
    )
    mean_abs_err = out.agg(F.mean("abs_error")).collect()[0][0]
    #   print(f"Test alpha {test_alpha} -> mean_abs_error (obs {n}) : {abs_err/n:.6f}")
    return out, mean_abs_err

# COMMAND ----------

sf, e = backtest_smooth_purchase_cyc_alpha(df, 0.2)
sf.display()

# COMMAND ----------

"""
Exponential Smoothing 2 data points
----
Backtest for best smoothing parameter (alpha)
"""

def find_min_alpha(df):
    """Find smooth factor that minimize mean abs error
    Hyperparam  from 0.3 - 0.75 with step 0.01
    """
    
    from matplotlib import pyplot as plt
    alpha = []
    mae = []
    for a in np.arange(0.3, 0.75, 0.01):
        alpha.append(a)
        _, err = backtest_smooth_purchase_cyc_alpha(df, test_alpha=a)
        mae.append(err)

    err_df = pd.DataFrame({"alpha":alpha, "mae":mae})
    fig, ax = plt.subplots()
    err_df.plot(x="alpha", y="mae", ax=ax)
    plt.title("Mean Absolute Error")
    plt.show()

    min_alpha = float(err_df.loc[err_df["mae"].idxmin(), "alpha"])

    return min_alpha

# COMMAND ----------

find_min_alpha(df)

# COMMAND ----------

def get_smooth_purchase_cyc(sf, alpha):
    """Get smoothed purchased cycle, based on exponential smoothing
    
    Exponential smoothing formula
    yt = alpha*y,t-1 + (1-alpha)*y,t-2
    """
    alpha = alpha
    alpha_1 = 1-alpha
    MAX_LATEST_NUM_PURCHASE = 2
    
    out = (sf
           .withColumn("desc_order", F.row_number().over(Window.partitionBy("household_id").orderBy(F.col("date_id").desc_nulls_last())))
           .where(F.col("desc_order")<=MAX_LATEST_NUM_PURCHASE)
           .groupBy("household_id")
           .pivot("desc_order")
           .agg(F.sum("day_diff"))
           .withColumn("smth_prchs_cyc", F.when(F.col("2").isNotNull(), F.col("1")*alpha + F.col("2")*alpha_1).otherwise(F.col("1")))
           .select("household_id", "smth_prchs_cyc")
    )

    return out  

# COMMAND ----------

min_alpha = find_min_alpha(df)
hh_smth_prchs_cyc = get_smooth_purchase_cyc(df, alpha=min_alpha)

# COMMAND ----------

hh_smth_prchs_cyc.display()

# COMMAND ----------

"""
Probability of next purchase fall into campaign period
----
Assume the next purchase event follow Exponentail distribution
"""
from datetime import datetime
from scipy.stats import expon

@udf("float")
def get_prob_next_purchase(purchase_cycle_day: float,
                           day_last_to_cmp_str: float,
                           day_last_to_cmp_end: float):
    """Get probability of next purchase
    will happened in campaign period
    """
    from scipy.stats import expon
    
    prob_til_cmp_str = expon(scale=purchase_cycle_day).cdf(x=day_last_to_cmp_str)
    prob_til_cmp_end = expon(scale=purchase_cycle_day).cdf(x=day_last_to_cmp_end)
    prob_in_cmp = prob_til_cmp_end - prob_til_cmp_str

    return float(prob_in_cmp)

# COMMAND ----------

"""
Extended probability of next N purchases fall into campaign period
----
For short purchase cycle, then then next purchase still not reach campaign start date
Assume that customer keep buying until purchase fall into campaing period
Assume each purchase cycle are independent, then overall propability = P(event_1)*P(event_2)*P(event_3)*...
"""
cmp_str_date_id = datetime.strptime("2022-08-01", "%Y-%m-%d")
cmp_period_day = 14

last_purchase_cyc = \
(df
 .groupBy("household_id")
 .agg(F.max("date_id").alias("last_purchase_date_id"))
 .join(hh_smth_prchs_cyc, "household_id", "outer")
)

nxt_purchase = \
(last_purchase_cyc
 .withColumn("day_last_prchs_to_cmp_str", F.datediff(F.lit(cmp_str_date_id) , F.col("last_purchase_date_id")))
 # number of purchase until next purchase fall into campaign
 .withColumn("num_rolling_purchase_before_cmp_str", F.expr(" int(floor(day_last_prchs_to_cmp_str / smth_prchs_cyc)) "))
 # prop of each rolling purchase
 .withColumn("prop_rolling_purchase", get_prob_next_purchase(F.col("smth_prchs_cyc"), F.lit(0.0), F.col("smth_prchs_cyc")))
 # last rolling purchase that the next will be in the campaign period
 .withColumn("rolling_purchase_before_cmp_str_date", F.expr(" date_add(last_purchase_date_id, int(floor(day_last_prchs_to_cmp_str / smth_prchs_cyc) * smth_prchs_cyc)) "))
 # Calculate 
 .withColumn("day_til_cmp_str", F.datediff(F.lit(cmp_str_date_id) , F.col("rolling_purchase_before_cmp_str_date")))
 .withColumn("day_til_cmp_end", F.col("day_til_cmp_str")+cmp_period_day)
 # all prop of N rolling purchase
 .withColumn("prop_prior_cmp", F.when(F.col("num_rolling_purchase_before_cmp_str")==0, F.lit(1.0) ).otherwise(F.pow(F.col("prop_rolling_purchase"), F.col("num_rolling_purchase_before_cmp_str"))))
 # last prob that purchase fall into campaign
 .withColumn("prop_pre_cmp", get_prob_next_purchase(F.col("smth_prchs_cyc"), F.col("day_til_cmp_str"), F.col("day_til_cmp_end")))
 # Final prop = all rolling prop x last prop 
 .withColumn("prop", F.col("prop_prior_cmp")*F.col("prop_pre_cmp"))
#  .withColumn("day_til_cmp_end_2", F.col("day_last_prchs_to_cmp_str")+cmp_period_day)
#  .withColumn("prop_2", get_prob_next_purchase(F.col("smth_prchs_cyc"), F.col("day_last_prchs_to_cmp_str"), F.col("day_til_cmp_end_2")))
)

# COMMAND ----------

nxt_purchase.display()

# COMMAND ----------


