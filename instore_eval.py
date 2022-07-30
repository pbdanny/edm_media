from typing import List
from copy import deepcopy
from datetime import datetime, timedelta
from pyspark import accumulators

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

spark = SparkSession.builder.appName("media_eval").getOrCreate()

def get_cust_activated(txn: SparkDataFrame,
                       cp_start_date: str,
                       cp_end_date: str,
                       wk_type: str,
                       test_store_sf: SparkDataFrame,
                       adj_prod_sf: SparkDataFrame,
                       brand_sf: SparkDataFrame,
                       feat_sf: SparkDataFrame):
    """Get customer exposed & unexposed / shopped, not shop

    Parameters
    ----------
    txn:
        Snapped transaction of ppp + pre + cmp period
    cp_start_date
    cp_end_date
    wk_type:
        "fis_week" or "promo_week"
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')

    #--- Helper fn
    def _get_period_wk_col_nm(wk_type: str
                              ) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo"]:
            period_wk_col_nm = "period_promo_wk"
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm

    def _create_test_store_sf(test_store_sf: SparkDataFrame,
                             cp_start_date: str,
                             cp_end_date: str
                             ) -> SparkDataFrame:
        """From target store definition, fill c_start, c_end
        based on cp_start_date, cp_end_date
        """
        filled_test_store_sf = \
            (test_store_sf
            .fillna(str(cp_start_date), subset='c_start')
            .fillna(str(cp_end_date), subset='c_end')
            )
        return filled_test_store_sf

    def _get_exposed_cust(txn: SparkDataFrame,
                          test_store_sf: SparkDataFrame,
                          adj_prod_sf: SparkDataFrame,
                          channel: str = "OFFLINE"
                          ) -> SparkDataFrame:
        """Get exposed customer & first exposed date
        """
        out = \
            (txn
             .where(F.col("channel")==channel)
             .where(F.col("household_id").isNotNull())
             .join(test_store_sf, "store_id","inner") # Mapping cmp_start, cmp_end, mech_count by store
             .join(adj_prod_sf, "upc_id", "inner")
             .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
             .groupBy("household_id")
             .agg(F.min("date_id").alias("first_exposed_date"))
            )
        return out

    def _get_shppr(txn: SparkDataFrame,
                   period_wk_col_nm: str,
                   prd_scope_df: SparkDataFrame
                   ) -> SparkDataFrame:
        """Get first brand shopped date or feature shopped date, based on input upc_id
        Shopper in campaign period at any store format & any channel
        """
        out = \
            (txn
             .where(F.col('household_id').isNotNull())
             .where(F.col(period_wk_col_nm).isin(["cmp"]))
             .join(prd_scope_df, 'upc_id')
             .groupBy('household_id')
             .agg(F.min('date_id').alias('first_shp_date'))
             .drop_duplicates()
            )
        return out

    def _get_activated(exposed_cust: SparkDataFrame,
                       shppr_cust: SparkDataFrame
                       ) -> SparkDataFrame:
        """Get activated customer : First exposed date <= First (brand/sku) shopped date
        """
        out = \
            (exposed_cust.join(shppr_cust, "household_id", "inner")
             .where(F.col('first_exposed_date').isNotNull())
             .where(F.col('first_shp_date').isNotNull())
             .where(F.col('first_exposed_date') <= F.col('first_shp_date'))
             .select("household_id")
             .drop_duplicates()
             )
        return out

    #---- Main
    print("-"*80)
    print("Customer Media Exposed -> Activated")
    print("Media Exposed = shopped in media aisle within campaign period (base on target input file) at target store , channel OFFLINE ")
    print("Activate = Exposed & Shop (Feature SKU/Feature Brand) in campaign period at any store format and any channel")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    # Brand activate
    target_str = _create_test_store_sf(test_store_sf=test_store_sf, cp_start_date=cp_start_date, cp_end_date=cp_end_date)
    cmp_exposed = _get_exposed_cust(txn=txn, test_store_sf=target_str, adj_prod_sf=adj_prod_sf)
    cmp_brand_shppr = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=brand_sf)
    cmp_brand_activated = _get_activated(exposed_cust=cmp_exposed, shppr_cust=cmp_brand_shppr)

    nmbr_brand_activated = cmp_brand_activated.count()
    print(f'Total exposed and Feature Brand (in Category scope) shopper (Brand Activated) : {nmbr_brand_activated:,d}')

    # Sku Activated
    cmp_sku_shppr = _get_shppr(txn=txn, period_wk_col_nm=period_wk_col, prd_scope_df=feat_sf)
    cmp_sku_activated = _get_activated(exposed_cust=cmp_exposed, shppr_cust=cmp_sku_shppr)

    nmbr_sku_activated = cmp_sku_activated.count()
    print(f'Total exposed and Features SKU shopper (Features SKU Activated) : {nmbr_sku_activated:,d}')

    return cmp_brand_activated, cmp_sku_activated

def get_cust_movement(txn: SparkDataFrame,
                      wk_type: str,
                      feat_sf: SparkDataFrame,
                      sku_activated: SparkDataFrame,
                      class_df: SparkDataFrame,
                      sclass_df: SparkDataFrame,
                      brand_df: SparkDataFrame,
                      switching_lv: str
                      ):
    """Customer movement based on tagged feature activated & brand activated

    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper function
    def _get_period_wk_col_nm(wk_type: str) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo"]:
            period_wk_col_nm = "period_promo_wk"
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm

    #---- Main
    # Movement
    # Existing and New SKU buyer (movement at micro level)
    print("-"*80)
    print("Customer movement")
    print("Movement consider only Feature SKU activated")
    print("-"*80)

    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    # Features SKU movement
    prior_pre_sku_shopper = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))
     .join(feat_sf, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )

    existing_exposed_cust_and_sku_shopper = \
    (sku_activated
     .join(prior_pre_sku_shopper, 'household_id', 'inner')
     .withColumn('customer_macro_flag', F.lit('existing'))
     .withColumn('customer_micro_flag', F.lit('existing_sku'))
     .checkpoint()
    )

    new_exposed_cust_and_sku_shopper = \
    (sku_activated
     .join(existing_exposed_cust_and_sku_shopper, 'household_id', 'leftanti')
     .withColumn('customer_macro_flag', F.lit('new'))
     .checkpoint()
    )

    # Customer movement for Feature SKU
    ## Macro level (New/Existing/Lapse)
    prior_pre_cc_txn = \
    (txn
     .where(F.col('household_id').isNotNull())
     .where(F.col(period_wk_col).isin(['pre', 'ppp']))
    )

    prior_pre_store_shopper = prior_pre_cc_txn.select('household_id').drop_duplicates()

    prior_pre_class_shopper = \
    (prior_pre_cc_txn
     .join(class_df, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )

    prior_pre_subclass_shopper = \
    (prior_pre_cc_txn
     .join(sclass_df, "upc_id", "inner")
     .select('household_id')
     .drop_duplicates()
    )

    ## Micro level
    new_sku_new_store = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_lotus'))
    )

    new_sku_new_class = \
    (new_exposed_cust_and_sku_shopper
     .join(prior_pre_store_shopper, 'household_id', 'inner')
     .join(prior_pre_class_shopper, 'household_id', 'leftanti')
     .select('household_id', 'customer_macro_flag')
     .withColumn('customer_micro_flag', F.lit('new_to_class'))
    )

    if switching_lv == 'subclass':
        new_sku_new_subclass = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_subclass'))
        )

        prior_pre_brand_in_subclass_shopper = \
        (prior_pre_cc_txn
         .join(sclass_df, "upc_id", "inner")
         .join(brand_df, "upc_id")
         .select('household_id')
         .drop_duplicates()
        )

        #---- Current subclass shopper , new to brand : brand switcher within sublass
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )

        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_subclass_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )

        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_subclass)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
         .checkpoint()
        )

        return result_movement, new_exposed_cust_and_sku_shopper

    elif switching_lv == 'class':

        prior_pre_brand_in_class_shopper = \
        (prior_pre_cc_txn
         .join(sclass_df, "upc_id", "inner")
         .join(brand_df, "upc_id")
         .select('household_id')
        ).drop_duplicates()

        #---- Current subclass shopper , new to brand : brand switcher within class
        new_sku_new_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_brand'))
        )

        new_sku_within_brand_shopper = \
        (new_exposed_cust_and_sku_shopper
         .join(prior_pre_store_shopper, 'household_id', 'inner')
         .join(prior_pre_class_shopper, 'household_id', 'inner')
         .join(prior_pre_brand_in_class_shopper, 'household_id', 'inner')
         .join(prior_pre_sku_shopper, 'household_id', 'leftanti')
         .select('household_id', 'customer_macro_flag')
         .withColumn('customer_micro_flag', F.lit('new_to_sku'))
        )

        result_movement = \
        (existing_exposed_cust_and_sku_shopper
         .unionByName(new_sku_new_store)
         .unionByName(new_sku_new_class)
         .unionByName(new_sku_new_brand_shopper)
         .unionByName(new_sku_within_brand_shopper)
         .checkpoint()
        )

        return result_movement, new_exposed_cust_and_sku_shopper

    else:
        print('Not recognized Movement and Switching level param')
        return None

def get_cust_brand_switching_and_penetration(
        txn: SparkDataFrame,
        switching_lv: str,
        brand_df: SparkDataFrame,
        class_df: SparkDataFrame,
        sclass_df: SparkDataFrame,
        cust_movement_sf: SparkDataFrame,
        wk_type: str,
        ):
    """Media evaluation solution, customer switching
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper fn
    def _get_period_wk_col_nm(wk_type: str) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo"]:
            period_wk_col_nm = "period_promo_wk"
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm

    ## Customer Switching by Sai
    def _switching(switching_lv:str, micro_flag: str, cust_movement_sf: SparkDataFrame,
                   prod_trans: SparkDataFrame, grp: List,
                   prod_lev: str, full_prod_lev: str ,
                   col_rename: str, period: str
                   ):
        """Customer switching from Sai
        """
        print(f'\t\t\t\t\t\t Switching of customer movement at : {micro_flag}')
        # List of customer movement at analysis micro level
        cust_micro_df = cust_movement_sf.where(F.col('customer_micro_flag') == micro_flag)
        prod_trans_cust_micro = prod_trans.join(cust_micro_df.select('household_id').dropDuplicates()
                                                , on='household_id', how='inner')
        cust_micro_kpi_prod_lv = \
        (prod_trans_cust_micro
         .groupby(grp)
         .agg(F.sum('net_spend_amt').alias('oth_'+prod_lev+'_spend'),
              F.countDistinct('household_id').alias('oth_'+prod_lev+'_customers'))
         .withColumnRenamed(col_rename, 'oth_'+full_prod_lev)
        )

        total_oth = \
        (cust_micro_kpi_prod_lv
         .agg(F.sum('oth_'+prod_lev+'_spend').alias('_total_oth_spend'))
        ).collect()[0][0]

        cust_micro_kpi_prod_lv = cust_micro_kpi_prod_lv.withColumn('total_oth_'+prod_lev+'_spend', F.lit(float(total_oth)))

        print("\t\t\t\t\t\t**Running micro df2")
        # Join micro df with prod trans
        if (prod_lev == 'brand') & (switching_lv == 'subclass'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name',
                      'class_name',
                      # 'subclass_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')

        elif (prod_lev == 'brand') & (switching_lv == 'class'):
            cust_micro_df2 = \
            (cust_micro_df
             .groupby('division_name','department_name','section_name','class_name',
                      F.col('brand_name').alias('original_brand'),
                      'customer_macro_flag','customer_micro_flag')
             .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
                  F.countDistinct('household_id').alias('total_ori_brand_cust'))
            )
            micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='class_name', how='inner')

        #---- To be done : if switching at multi class
        # elif prod_lev == 'class':
        #     micro_df_summ = cust_micro_df2.join(cust_micro_kpi_prod_lv, on='section_name', how='inner')
        else:
            micro_df_summ = spark.createDataFrame([],[])

        print("\t\t\t\t\t\t**Running Summary of micro df")
        switching_result = \
        (micro_df_summ
         .select('division_name','department_name','section_name','class_name','original_brand',
                 'customer_macro_flag','customer_micro_flag','total_ori_brand_cust','total_ori_brand_spend',
                 'oth_'+full_prod_lev,'oth_'+prod_lev+'_customers','oth_'+prod_lev+'_spend','total_oth_'+prod_lev+'_spend')
         .withColumn('pct_cust_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_customers')/F.col('total_ori_brand_cust'))
         .withColumn('pct_spend_oth_'+full_prod_lev, F.col('oth_'+prod_lev+'_spend')/F.col('total_oth_'+prod_lev+'_spend'))
         .orderBy(F.col('pct_cust_oth_'+full_prod_lev).desc(), F.col('pct_spend_oth_'+full_prod_lev).desc())
        )
        switching_result = switching_result.checkpoint()

        return switching_result

    def _get_swtchng_pntrtn(switching_lv: str):
        """Get Switching and penetration based on defined switching at class / subclass
        Support multi subclass
        """
        if switching_lv == "subclass":
            prd_scope_df = sclass_df
            gr_col = ['division_name','department_name','section_name','class_name',
                      'brand_name','household_id']
        else:
            prd_scope_df = class_df
            gr_col = ['division_name','department_name','section_name',
                      'brand_name','household_id']

        prior_pre_cc_txn_prd_scope = \
        (txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col).isin(['pre', 'ppp']))
         .join(prd_scope_df, "upc_id", "inner")
        )

        prior_pre_cc_txn_prd_scope_sel_brand = prior_pre_cc_txn_prd_scope.join(brand_df, "upc_id", "inner")

        prior_pre_prd_scope_sel_brand_kpi = \
        (prior_pre_cc_txn_prd_scope_sel_brand
         .groupBy(gr_col)
         .agg(F.sum('net_spend_amt').alias('brand_spend_pre'))
        )

        dur_cc_txn_prd_scope = \
        (txn
         .where(F.col('household_id').isNotNull())
         .where(F.col(period_wk_col).isin(['cmp']))
         .join(prd_scope_df, "upc_id", "inner")
        )

        dur_cc_txn_prd_scope_sel_brand = dur_cc_txn_prd_scope.join(brand_df, "upc_id", "inner")

        dur_prd_scope_sel_brand_kpi = \
        (dur_cc_txn_prd_scope_sel_brand
         .groupBy(gr_col)
         .agg(F.sum('net_spend_amt').alias('brand_spend_dur'))
        )

        pre_dur_band_spend = \
        (prior_pre_prd_scope_sel_brand_kpi
         .join(dur_prd_scope_sel_brand_kpi, gr_col, 'outer')
        )

        cust_movement_pre_dur_spend = cust_movement_sf.join(pre_dur_band_spend, 'household_id', 'left')
        new_to_brand_switching_from = _switching(switching_lv, 'new_to_brand',
                                                 cust_movement_pre_dur_spend,
                                                 prior_pre_cc_txn_prd_scope,
                                               # ['subclass_name', 'brand_name'],
                                                 ["class_name", 'brand_name'],
                                                 'brand', 'brand_in_category', 'brand_name', 'dur')
        # Brand penetration within subclass
        dur_prd_scope_cust = dur_cc_txn_prd_scope.agg(F.countDistinct('household_id')).collect()[0][0]
        brand_cust_pen = \
        (dur_cc_txn_prd_scope
         .groupBy('brand_name')
         .agg(F.countDistinct('household_id').alias('brand_cust'))
         .withColumn('category_cust', F.lit(dur_prd_scope_cust))
         .withColumn('brand_cust_pen', F.col('brand_cust')/F.col('category_cust'))
        )

        return new_to_brand_switching_from, cust_movement_pre_dur_spend, brand_cust_pen

    #---- Main
    print("-"*80)
    print("Customer brand switching")
    print(f"Brand switching within : {switching_lv.upper()}")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    new_to_brand_switching, cust_mv_pre_dur_spend, brand_cust_pen = _get_swtchng_pntrtn(switching_lv=switching_lv)
    cust_brand_switching_and_pen = \
        (new_to_brand_switching.alias("a")
         .join(brand_cust_pen.alias("b"),
               F.col("a.oth_brand_in_category")==F.col("b.brand_name"), "left")
        )

    return new_to_brand_switching, brand_cust_pen, cust_brand_switching_and_pen

def get_cust_sku_switching(
        txn: SparkDataFrame,
        switching_lv: str,
        sku_activated: SparkDataFrame,
        feat_list: List,
        class_df: SparkDataFrame,
        sclass_df: SparkDataFrame,
        wk_type: str,
        ):
    """Media evaluation solution, customer sku switching
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    #---- Helper fn
    def _get_period_wk_col_nm(wk_type: str) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo"]:
            period_wk_col_nm = "period_promo_wk"
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm

    #---- Main
    print("-"*80)
    print("Customer switching SKU for 'OFFLINE' + 'ONLINE'")
    print(f"Switching within : {switching_lv.upper()}")
    print("Customer Movement consider only Feature SKU activated")
    print("-"*80)

    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)

    if switching_lv == "subclass":
        cat_df = sclass_df
    else:
        cat_df = class_df

    prod_desc = spark.table('tdm.v_prod_dim_c').select('upc_id', 'product_en_desc').drop_duplicates()

    # (PPP+Pre) vs Dur Category Sales of each household_id
    ## Windows aggregate style
    txn_per_dur_cat_sale = \
        (txn
            .filter(F.col('household_id').isNotNull())
            .filter(F.col(period_wk_col).isin(['pre', 'ppp', 'cmp']))
            .join(cat_df, "upc_id", "inner")
            .withColumn('pre_cat_sales', F.when( F.col(period_wk_col).isin(['ppp', 'pre']) , F.col('net_spend_amt') ).otherwise(0) )
            .withColumn('dur_cat_sales', F.when( F.col(period_wk_col).isin(['cmp']), F.col('net_spend_amt') ).otherwise(0) )
            .withColumn('cust_tt_pre_cat_sales', F.sum(F.col('pre_cat_sales')).over(Window.partitionBy('household_id') ))
            .withColumn('cust_tt_dur_cat_sales', F.sum(F.col('dur_cat_sales')).over(Window.partitionBy('household_id') ))
            )

    txn_cat_both = txn_per_dur_cat_sale.filter( (F.col('cust_tt_pre_cat_sales')>0) & (F.col('cust_tt_dur_cat_sales')>0) )

    txn_cat_both_sku_only_dur = \
    (txn_cat_both
        .withColumn('pre_sku_sales',
                    F.when( (F.col(period_wk_col).isin(['ppp', 'pre'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
        .withColumn('dur_sku_sales',
                    F.when( (F.col(period_wk_col).isin(['cmp'])) & (F.col('upc_id').isin(feat_list)), F.col('net_spend_amt') ).otherwise(0) )
        .withColumn('cust_tt_pre_sku_sales', F.sum(F.col('pre_sku_sales')).over(Window.partitionBy('household_id') ))
        .withColumn('cust_tt_dur_sku_sales', F.sum(F.col('dur_sku_sales')).over(Window.partitionBy('household_id') ))
        .filter( (F.col('cust_tt_pre_sku_sales')<=0) & (F.col('cust_tt_dur_sku_sales')>0) )
    )

    n_cust_switch_sku = \
    (txn_cat_both_sku_only_dur
        .filter(F.col('pre_cat_sales')>0) # only other products
        .join(sku_activated, 'household_id', 'inner')
        .groupBy('upc_id')
        .agg(F.countDistinct('household_id').alias('custs'))
        .join(prod_desc, 'upc_id', 'left')
        .orderBy('custs', ascending=False)
    )

    return n_cust_switch_sku

def map_cust_truprice(txn: SparkDataFrame,
                      store_fmt: str,
                      cmp_end_date: str,
                      wk_type: str,
                      sku_activated: SparkDataFrame,
                      switching_lv: str,
                      class_df: SparkDataFrame,
                      sclass_df: SparkDataFrame
                      ):
    """Profile activated customer based on TruPrice segment
    Compare with total Lotus shopper at same store format
    """
    #---- Helper fn
    def _get_period_wk_col_nm(wk_type: str
                              ) -> str:
        """Column name for period week identification
        """
        if wk_type in ["promo"]:
            period_wk_col_nm = "period_promo_wk"
        else:
            period_wk_col_nm = "period_fis_wk"
        return period_wk_col_nm

    def _get_truprice_seg(cmp_end_date: str):
        """Get truprice seg from campaign end date
        With fallback period_id in case truPrice seg not available
        """
        from datetime import datetime, date, timedelta

        def __get_p_id(date_id: str,
                       bck_days: int = 0)-> str:
            """Get period_id for current or back date
            """
            date_dim = spark.table("tdm.v_date_dim")
            bck_date = (datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=bck_days)).strftime("%Y-%m-%d")
            bck_date_df = date_dim.where(F.col("date_id")==bck_date)
            bck_p_id = bck_date_df.select("period_id").drop_duplicates().collect()[0][0]

            return bck_p_id

        # Find period id to map Truprice / if the truprice period not publish yet use latest period
        bck_p_id = __get_p_id(cmp_end_date, bck_days=180)
        truprice_all = \
            (spark.table("tdm_seg.srai_truprice_full_history")
             .where(F.col("period_id")>=bck_p_id)
             .select("household_id", "truprice_seg_desc", "period_id")
             .drop_duplicates()
            )
        max_trprc_p_id = truprice_all.agg(F.max("period_id")).drop_duplicates().collect()[0][0]

        crrnt_p_id = __get_p_id(cmp_end_date, bck_days=0)

        if int(max_trprc_p_id) < int(crrnt_p_id):
            trprc_p_id = max_trprc_p_id
        else:
            trprc_p_id = crrnt_p_id

        trprc_seg = \
            (truprice_all
             .where(F.col("period_id")==trprc_p_id)
             .select("household_id", "truprice_seg_desc")
            )

        return trprc_seg, trprc_p_id

    def _get_truprice_cust_pen(txn: SparkDataFrame,
                               lv_nm: str):
        """Group by truprice , calculate cust penetration
        """
        tp_pen = \
            (txn
             .groupBy("truprice_seg_desc")
             .agg(F.countDistinct("household_id").alias(f"{lv_nm}_cust"))
             .withColumn(f"total_{lv_nm}_cust", F.sum("cust").over(Window.partitionBy()))
             .withColumn(f"{lv_nm}_cust_pen", F.col(f"{lv_nm}_cust")/F.col(f"total_{lv_nm}_cust"))
            )
        return tp_pen

    #---- Main
    print("-"*80)
    print("Profile activated customer : TruPrice")
    print(f"Index with Total Lotus & {switching_lv.upper()} shopper at format : {store_fmt.upper()}, OFFLINE + ONLINE")
    print("-"*80)
    period_wk_col = _get_period_wk_col_nm(wk_type=wk_type)
    print(f"Period PPP / PRE / CMP based on column {period_wk_col}")
    print("-"*80)
    truprice_seg, truprice_period_id = _get_truprice_seg(cmp_end_date=cmp_end_date)
    print(f"TruPrice Segment Period Id : {truprice_period_id}")
    print("-"*80)

    # Map truprice at Lotus / prod scope
    if switching_lv == "subclass":
        prd_scope_df = sclass_df
    else:
        prd_scope_df = class_df

    txn_truprice = \
        (txn
         .where(F.col("household_id").isNotNull())
         .join(truprice_seg, "household_id", "left")
         .fillna(value="Unidentifed", subset=["truprice_seg_desc"])
         )
    txn_fmt = \
        (txn_truprice
         .where(F.col(period_wk_col).isin(["cmp"]))
         .where(F.upper(F.col("store_format_group"))==store_fmt.upper())
        )
    txn_fmt_prd_scp = txn_fmt.join(prd_scope_df, "upc_id")
    txn_actvtd = txn_fmt.join(sku_activated, "household_id", "inner")

    # Customer penetration by TruPrice
    fmt_tp = _get_truprice_cust_pen(txn_fmt, store_fmt.lower())
    prd_scp_tp = _get_truprice_cust_pen(txn_fmt_prd_scp, switching_lv.lower())
    actvtd_tp = _get_truprice_cust_pen(txn_actvtd, "sku_activated")
    combine_tp = fmt_tp.join(prd_scp_tp, "truprice_seg_desc", "left").join(actvtd_tp, "truprice_seg_desc", "left")

    # calculate index
    idx_tp = \
        (combine_tp
         .withColumn(f"idx_{store_fmt.lower()}", F.col(f"sku_activated_cust_pen")/F.col(f"{store_fmt.lower()}_cust_pen"))
         .withColumn(f"idx_{switching_lv.lower()}", F.col(f"sku_activated_cust_pen")/F.col(f"{switching_lv.lower()}_cust_pen"))
        )
    return idx_tp

def main():
    pass

if __name__ == "__main__":
    main()