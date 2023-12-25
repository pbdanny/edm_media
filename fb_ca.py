def _get_last_dt_id_txt(wk_id: int) -> str:
    """
    """
    dt_id = spark.table("tdm.v_th_date_dim").where(F.col("week_id")==wk_id).agg(F.max("date_id")).collect()[0][0]
    dt_id_txt = dt_id.strftime("%Y-%m-%d")
    return dt_id_txt

def _get_truprice_seg(cp_end_date: str):
    """Get truprice seg from campaign end date
    With fallback period_id in case truPrice seg not available
    """
    from datetime import datetime, date, timedelta

    def __get_p_id(date_id: str,
                   bck_days: int = 0)-> str:
        """Get period_id for current or back date
        """
        date_dim = spark.table("tdm.v_th_date_dim")
        bck_date = (datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=bck_days)).strftime("%Y-%m-%d")
        bck_date_df = date_dim.where(F.col("date_id")==bck_date)
        bck_p_id = bck_date_df.select("period_id").drop_duplicates().collect()[0][0]

        return bck_p_id

    # Find period id to map Truprice / if the truprice period not publish yet use latest period
    bck_p_id = __get_p_id(cp_end_date, bck_days=180)
    truprice_all = \
        (spark.table("tdm_seg.srai_truprice_full_history")
         .where(F.col("period_id")>=bck_p_id)
         .select("household_id", "truprice_seg_desc", "period_id")
         .drop_duplicates()
        )
    max_trprc_p_id = truprice_all.agg(F.max("period_id")).drop_duplicates().collect()[0][0]

    crrnt_p_id = __get_p_id(cp_end_date, bck_days=0)

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

def get_to_alloc(alloc_spc,
                 rank_map_table: str,
                 allocated_table: str,
                 reset: bool = False
                ):
    """Calculate n to allocated and order of allocation
    Param
    -----
    alloc_spc : allocation specification
    rank_map_table : original rank map table
    allocated_table : result of allcation, stampped column "ca_nm"
    reset : if True, will start over from rank map table
    """
    if not reset:
        try:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
        except Exception as e:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))
    else:
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))

    n_allctd = allctd.groupBy("ca_nm").agg(F.count("*").alias("n_allctd"))

    to_allctd = \
    (alloc_spc
     .join(n_allctd, "ca_nm", "left")
     .fillna(0, subset="n_allctd")
     .withColumn("to_alloc", F.col("n_ca") - F.col("n_allctd"))
     .withColumn("prop_to_alloc", F.col("to_alloc")/F.col("n_ca"))
     .orderBy(F.col("prop_to_alloc").desc(), F.col("ca_nm"))
    )
    to_allctd.display()
    to_allctd_df = to_allctd.toPandas()

    return to_allctd, to_allctd_df

def chk_size_combi_allctd(n: int,
                          rank_map_table: str,
                          allocated_table: str,
                          reset: bool = False
                         ):
    """Check if n_gr (size of combination) still available for allocation
    Param
    -----
    n : size of combination
    rank_map_table : original rank map table
    allocated_table : result of allcation, stampped column "ca_nm"
    reset : if True, will start over from rank map table
    """
    if not reset:
        try:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
        except Exception as e:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))
    else:
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None))

    n_avlbl = allctd.where(F.col("ca_nm").isNull()).where(F.col("n_gr")==n).count()

    return n_avlbl

def update_allocation(allocated_sf: SparkDataFrame,
                      allocated_table: SparkDataFrame,
                     ):
    """Update allocation based on result of allocated_sf
    """
    allctd = allocated_table
    with_ca = allctd.where(F.col("ca_nm").isNotNull())
    no_ca = allctd.where(F.col("ca_nm").isNull())

    # update allocation
    fill_ca = \
    (no_ca.alias("a")
     .join(allocated_sf.select("household_id", "alloc_ca_nm").alias("b"), "household_id", "left")
     .withColumn("ca_nm", F.coalesce(F.col("b.alloc_ca_nm"), F.col("a.ca_nm")))
     .drop("alloc_ca_nm")
    )

    # Create new allctd
    new_allctd = with_ca.unionByName(fill_ca)

    return new_allctd

def allocation_ca_nm(n: int,
                     to_alloctd_df: PandasDataFrame,
                     rank_map_table: str,
                     allocated_table: str,
                     reset: bool = False
                    ):
    """
    Param
    -----
    n : size of combination
    to_alloctd_df : Order of allocation
    rank_map_table : original rank map table
    allocated_table : result of allcation, stampped column "ca_nm"
    reset : if True, will start over from rank map table
    """
    if not reset:
        try:
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
        except Exception as e:
            new_allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None).cast(T.StringType()))
            new_allctd.write.format('parquet').mode("overwrite").save(os.path.join(prjct_abfss_prefix, allocated_table))
            allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))
    else:
        print(f"Reset allocated table, deleting allocated table at : {os.path.join(prjct_abfss_prefix, allocated_table)}")
        dbutils.fs.rm(os.path.join(prjct_abfss_prefix, allocated_table), True)
        new_allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, rank_map_table)).withColumn("ca_nm", F.lit(None).cast(T.StringType()))
        new_allctd.write.format('parquet').mode("overwrite").save(os.path.join(prjct_abfss_prefix, allocated_table))
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))

    n_gr = n
    step_allctd_sfs = []

    for r in to_allctd_df.itertuples():
        print(f"Combination size : {n_gr}")
        print(f"'{r.gr_nm}' => '{r.ca_nm}' target to allocate {r.to_alloc:,d}")

        # reload allocated from previous loop
        allctd = spark.read.parquet(os.path.join(prjct_abfss_prefix, allocated_table))

        filtered_key = \
        (allctd
         .where(F.col("ca_nm").isNull())
         .where(F.col("n_gr")==n_gr)
         # filter map with specific key value
         .withColumn("filter_map", F.map_filter("gr_rank_map", lambda k, v : k == r.gr_nm))
         .where(F.size("filter_map")>0)
        )

        print(f"gr_nm '{r.gr_nm}' have {filtered_key.count():,d} records")
        rank = \
        (filtered_key
         # extract value from map pair
         .withColumn("rank", F.explode(F.map_values(F.col("filter_map"))))
         .orderBy(F.col("rank"))
         .limit(r.to_alloc)
         .withColumn("alloc_ca_nm", F.lit(r.ca_nm))
        )

        step_allctd_sfs.append(rank)

        # update allocation to allocated table
        new_allocated = update_allocation(rank, allctd)
        new_allocated.write.format('parquet').mode("overwrite").save(os.path.join(prjct_abfss_prefix, allocated_table))

    return step_allctd_sfs

#---- Stratify sampling 5% by TruPrice
fraction = {'Most Price Driven':0.05, 'Most Price Insensitive':0.05, 'Price Driven':0.05,
            'Price Insensitive':0.05, 'Price Neutral':0.05, 'No TruPrice segment data':0.05}

def statify_2_step(sf, macro_group_col_name, startum_col_name, fraction_dict):
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')

    macro_group_list = sf.select(F.col(macro_group_col_name)).drop_duplicates().toPandas()[macro_group_col_name].to_list()

    test_sf = spark.createDataFrame([], sf.schema)
    control_sf = spark.createDataFrame([], sf.schema)
    for gr in macro_group_list:
        gr_whole = sf.filter(F.col(macro_group_col_name)==gr)
        gr_whole = gr_whole.checkpoint()

        gr_ctrl = gr_whole.sampleBy(startum_col_name, fractions=fraction_dict, seed=0)
        gr_test = gr_whole.subtract(gr_ctrl)

        gr_test = gr_test.checkpoint()
        gr_ctrl = gr_ctrl.checkpoint()

        test_sf = test_sf.unionByName(gr_test)
        control_sf = control_sf.unionByName(gr_ctrl)

    return test_sf, control_sf

def map_golden_export_csv(sf):
    """From sparkFrame, map golden record
    """
    cust_golden = spark.table("tdm.v_customer_dim").select('household_id', 'golden_record_external_id_hash').drop_duplicates()
    sf_out = sf.join(cust_golden, "household_id", "left")

    return sf_out

def map_group_golden_to_csv(sf, gr_col_nm, dbfs_python_prefix, file_nm_suffix):
    """Get list of group
    Map golden, save to target location as single csv with file name = group_name + suffix
    """
    gr_list = sf.select(gr_col_nm).drop_duplicates().toPandas()[gr_col_nm].to_list()
    sf_gl = map_golden_export_csv(sf)

    for gr in gr_list:
        csv_file_nm = gr + "_" + file_nm_suffix + ".csv"
        sf_to_export = sf_gl.where(F.col(gr_col_nm)==gr).select("golden_record_external_id_hash")
        df_to_export = sf_to_export.toPandas()
        print(f"Convert {gr} to csv at location {dbfs_python_prefix} with file name {csv_file_nm}")
        pandas_to_csv_filestore(df_to_export, csv_file_name=csv_file_nm, prefix=dbfs_python_prefix)

target_spec = [
{'gr_alloc_nm':'10_Nescafe_GoFresh','limit':128000},
{'gr_alloc_nm':'11_NescafeGold_GoFresh','limit':36000},
{'gr_alloc_nm':'12_PurelifeMinere_GoFresh','limit':63000},
{'gr_alloc_nm':'13_Milo_GoFresh','limit':45000},
{'gr_alloc_nm':'14_MultiBrand_GoFresh','limit':45000},
{'gr_alloc_nm':'2_Nescafe_HDE','limit':264000},
{'gr_alloc_nm':'3_NescafeGold_HDE','limit':45000},
{'gr_alloc_nm':'4_PurelifeMinere_HDE','limit':120000},
{'gr_alloc_nm':'5_Milo_HDE','limit':45000},
{'gr_alloc_nm':'6_Nestvita_HDE','limit':45000},
{'gr_alloc_nm':'7_MultiBrand_HDE','limit':45000},
{'gr_alloc_nm':'8_CompetitorNescafe_HDE','limit':50000},
{'gr_alloc_nm':'9_CompetitorMilo_HDE','limit':50000},
]

def limit_gr_rank(sf, spec):
    """
    """
    spark.sparkContext.setCheckpointDir('dbfs:/FileStore/thanakrit/temp/checkpoint')
    limited_sf = spark.createDataFrame([], sf.schema)

    for s in spec:
        print(f'{s["gr_alloc_nm"]} limit {s["limit"]}')
        gr_sf = sf.where(F.col("gr_alloc_nm")==s["gr_alloc_nm"]).orderBy(F.col("sales_rank_in_gr")).limit(s["limit"])
        gr_sf = gr_sf.checkpoint()
        limited_sf = limited_sf.unionByName(gr_sf)
        limited_sf = limited_sf.checkpoint()

    return limited_sf