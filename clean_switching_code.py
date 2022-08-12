## Customer Switching by Sai
from pyspark.sql import Window
from pyspark.sql.functions import greatest


def _switching(cust_mv_brand_spend: SparkDataFrame,
               cust_mv_micro_flag: str,
               switching_lv:str,
               txn_feat_ctgry_pre: SparkDataFrame,
            #    grpy_lvl: str,
               agg_lvl: str,
               agg_lvl_re_nm: str ,
               period: str
               ):
    """Customer switching from Sai
    switching_lv : class, sublass
    micro_flag : "new_to_brand"
    cust_mv_brand_spend : SparkDataFrame of hh_id, customer_macro_flag, *customer_micro_flag, all hierachy, brand spend for pre - dur
    txn_feat_ctgry_pre : all txn in prior+pre period of feature subclass/class based on switching_lv

    grp : fix ['class_name', 'brand_name'] ; for group by pri+pre sales, n_cust
    prod_lev: fix 'brand' ; for column name of kpi => oth_{}_spend , => oth_{}_custs
    full_prod_lev: fix 'brand_in_category' ;
    col_rename: fix 'brand_name' ; rename in param `grp` ['class_name', 'brand_name'] =>  ['class_name', 'oth_brand_in_category'']

    period: 'dur'
    """
    print("-"*80)
    print(f"Customer Movement Micor Level Group : '{cust_mv_micro_flag}'")
    print(f"Switching of '{agg_lvl}' in '{switching_lv}'")

    # filter hh_id, brand spend pre, dur of desired movement micro_flag, defaut 'new_to_brand'
    cust_mv_micro_brand_spend = cust_mv_brand_spend.where(F.col('customer_micro_flag') == cust_mv_micro_flag)

    # txn in Pri+Pre of features subclass/class, for cust mv micro gr
    txn_cust_mv_micro_pre = \
        (txn_feat_ctgry_pre
         .join(cust_mv_micro_brand_spend.select('household_id').dropDuplicates(), on='household_id', how='inner')
        )
    # Agg sales, n_cust of `higher 1 lvl`, brand name for cust micro mv group in pre period
    # if switching = `class` -> Agg at section (to support multi class switching) and so on
    # change column "brand_name" -> oth_brand_in_category

    if switching_lv == "class":
        higher_lvl = "section_name"
        higher_grpby_lvl = ["section_name", agg_lvl]
        dur_brand_spend_grpby = ['division_name','department_name','section_name',
                                 F.col("brand_name").alias("original_brand"),
                                 'customer_macro_flag','customer_micro_flag']
    elif switching_lv == "subclass":
        higher_lvl = "class_name"
        higher_grpby_lvl = ["class_name", agg_lvl]
        dur_brand_spend_grpby = ['division_name','department_name','section_name',
                                 "class_name",
                                 F.col("brand_name").alias("original_brand"),
                                 'customer_macro_flag','customer_micro_flag']
    else:
        print("Use default class switching result")
        higher_lvl = "class_name"
        higher_grpby_lvl = ["class_name", agg_lvl]
        dur_brand_spend_grpby = ['division_name','department_name','section_name',
                                 F.col("brand_name").alias("original_brand"),
                                 'customer_macro_flag','customer_micro_flag']

    print(f"To support multi-{switching_lv} switching, Aggregate of brand 1-higher lv at : ", higher_grpby_lvl)

    cust_mv_higher_lvl_kpi_pre = \
    (txn_cust_mv_micro_pre
     .groupby(higher_grpby_lvl)
     .agg(F.sum('net_spend_amt').alias(f'oth_{agg_lvl}_spend'),
          F.countDistinct('household_id').alias(f'oth_{agg_lvl}_customers'),
          F.sum("net_spend_amt").over(Window.partitionBy()).alias(f"total_oth_{agg_lvl}_spend"))
     .withColumnRenamed(agg_lvl, f'oth_{agg_lvl_re_nm}')
    )

    # Agg during period, brand spend
    cust_mv_micro_dur_brand_kpi = \
    (cust_mv_micro_brand_spend
        .groupby(dur_brand_spend_grpby)
        .agg(F.sum('brand_spend_'+period).alias('total_ori_brand_spend'),
            F.countDistinct('household_id').alias('total_ori_brand_cust'))
    )
    cust_mv_kpi_pre_dur = cust_mv_micro_dur_brand_kpi.join(cust_mv_higher_lvl_kpi_pre,
                                                            on=higher_lvl, how='inner')
    # Select column, order by pct_cust_oth
    if switching_lv == "class":
        sel_col = ['division_name','department_name','section_name',
                   "original_brand", 'customer_macro_flag','customer_micro_flag',
                   'total_ori_brand_cust','total_ori_brand_spend',
                   'oth_'+agg_lvl_re_nm,'oth_'+agg_lvl+'_customers',
                   'oth_'+agg_lvl+'_spend','total_oth_'+agg_lvl+'_spend']
    elif switching_lv == "subclass":
        sel_col = ['division_name','department_name','section_name',
                   "class_name",
                   "original_brand", 'customer_macro_flag','customer_micro_flag',
                   'total_ori_brand_cust','total_ori_brand_spend',
                   'oth_'+agg_lvl_re_nm,'oth_'+agg_lvl+'_customers',
                   'oth_'+agg_lvl+'_spend','total_oth_'+agg_lvl+'_spend']
    else:
        sel_col = ['division_name','department_name','section_name',
                   "original_brand", 'customer_macro_flag','customer_micro_flag',
                   'total_ori_brand_cust','total_ori_brand_spend',
                   'oth_'+agg_lvl_re_nm,'oth_'+agg_lvl+'_customers',
                   'oth_'+agg_lvl+'_spend','total_oth_'+agg_lvl+'_spend']

    switching_result = \
    (cust_mv_kpi_pre_dur
     .select(sel_col)
     .withColumn('pct_cust_oth_'+agg_lvl_re_nm, F.col('oth_'+agg_lvl+'_customers')/F.col('total_ori_brand_cust'))
     .withColumn('pct_spend_oth_'+agg_lvl_re_nm, F.col('oth_'+agg_lvl+'_spend')/F.col('total_oth_'+agg_lvl+'_spend'))
     .orderBy(F.col('pct_cust_oth_'+agg_lvl_re_nm).desc())
    )
    switching_result = switching_result.checkpoint()

    return switching_result