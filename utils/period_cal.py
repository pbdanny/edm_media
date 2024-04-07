import math
from datetime import datetime, timedelta

from utils.campaign_config import CampaignEval, CampaignEvalO3


def first_date_of_wk(in_dt):
    """
    Description: To get the first date of week from input date
        Input parameter: in_dt as string in format yyyy-mm-dd
        ReTurn: First Date of week of the provided date

    """
    ## convert input string to date
    in_date = datetime.strptime(in_dt, "%Y-%m-%d")

    ## get week day number
    in_wk_day = in_date.strftime("%w")

    ## Get diff from current date to first day of week.
    ## Sunday is "0" reset value to 7 (last day of week for Lotus)
    if int(in_wk_day) == 0:
        diff_day = (int(in_wk_day) + 7) - 1
    else:
        diff_day = int(in_wk_day) - 1
    ## end if

    fst_dt_wk = in_date - timedelta(days=diff_day)

    return fst_dt_wk


def wk_of_year_ls (in_dt):
    """
    Description: Get week of specific year (Lotus Week) from specific date
    Input Parameter :
        in_dt: string , string of date in format yyyy-mm-dd --> '%Y-%m-%d'
    Prerequsit function: first_date_of_wk(in_dt)
    """
    in_date = datetime.strptime(in_dt, '%Y-%m-%d')
    
    ## get year, weekday number
    in_yr   = int(in_date.strftime('%y'))  ## return last 2 digit of year ex: 22
    in_year = int(in_date.strftime('%Y'))
    
    ## create next year value    
    in_nxyr   = int(in_yr) + 1
    in_nxyear = int(in_year) + 1
    
    ## Step 0: Prepre first date of this year and nextyear
    ## For Lotus first week of year is week with 30th Dec except year 2024, first date of year = 2024-01-01 ** special request from Lotuss
    
    if in_year == 2023 :
        
        inyr_1date = first_date_of_wk(str(in_year -1) + '-' + '12-30')
        nxyr_1date = first_date_of_wk(str(in_nxyear) + '-' + '01-01')
    elif in_year == 2024:
        inyr_1date = first_date_of_wk(str(in_year) + '-' + '01-01')
        nxyr_1date = first_date_of_wk(str(in_nxyear -1) + '-' + '12-30')
    else:
        inyr_1date = first_date_of_wk(str(in_year -1) + '-' + '12-30')
        nxyr_1date = first_date_of_wk(str(in_nxyear -1) + '-' + '12-30')
    
    ## end if
    
    ## Step 1 : Check which year to get first date of year
    ## Step 2 : Get first date of year and diff between current date  THen get week
    
    if in_date < nxyr_1date:
        fdate_yr = inyr_1date
        
        dt_delta = in_date - fdate_yr
        
        ## get number of days different from current date        
        dt_diff  = dt_delta.days
        
        ## Convert number of days to #weeks of year (+ 7 to ceiling)
        wk    = int((dt_diff + 7)/7)
        
        ## multiply 100 to shift bit + number of week
        
        fiswk = (in_year * 100)+ wk
        
    else:
        fdate_yr = nxyr_1date
        ## return first week of year
        fiswk    = (int(in_nxyear) * 100) + 1 
    ## end if
    
    return fiswk

## end def


def f_date_of_wk(in_wk):
    """
    input parameter = in_wk is week_id eg: 202203
    prerequsit Function :
    first_date_of_wk(in_date)
    """
    # Step : get year, week of year using mod
    wk = in_wk % 100
    
    ## 4 digit year
    in_yr     = (in_wk - wk)/100
    
    diff_days = (wk-1) * 7  ## diff date 
    
    ## step2 get date from first date of year (in first week)
    
    if in_yr == 2024:
        fdate_yr = first_date_of_wk(str(int(in_yr)) + '-01-01')
    else :
        fdate_yr = first_date_of_wk(str(int(in_yr)-1) + '-12-31')
    ## end if
    
    dt_wk    = fdate_yr + timedelta(days=diff_days)
    
    return dt_wk

## End def

def week_cal (in_wk, n) :

    """
    Description : Get week apart from "in_wk" by "n" (weeks)
    Input parameter : 2
    in_wk = week number format yyyywk
    n     = number of weeks apart from "in_wk' - exclusive in_wk (if minus then week before)
    Prerequsit function:
    first_date_of_wk, wkOfYearLs, fDateofWk
    
    """
    
    in_dt   = f_date_of_wk(in_wk)
    diff_dy = n * 7
    
    out_dt  = in_dt + timedelta(days=diff_dy)
    out_wk = wk_of_year_ls(out_dt.strftime('%Y-%m-%d'))
    
    return out_wk

## End def

def first_date_of_promo_wk (in_dt):
    """
    Description: To get the first date of promo week from input date
    Input parameter: in_dt as string in format yyyy-mm-dd
    ReTurn: First Date of week of the provided date
    
    """
    ## convert input string to date    
    in_date   = datetime.strptime(in_dt, '%Y-%m-%d')
    
    ## get week day number
    in_wk_day = in_date.strftime('%w')
    
    ## Get diff from current date to first day of week.
    ## Sunday is "0" reset value to 4 (promo week start from Thursday = 4)    
    if int(in_wk_day) == 4:
        diff_day = 0
    else:
        diff_day = ((int(in_wk_day) + 4) -1 ) % 7  ## mod
    ## end if
    
    fst_dt_wk = in_date - timedelta(days = diff_day)
    
    return fst_dt_wk


def wk_of_year_promo_ls (in_dt):
    """
    Description: Get week of specific year (Lotus Week) from specific date
    Input Parameter :
        in_dt: string , string of date in format yyyy-mm-dd --> '%Y-%m-%d'
    Prerequsit function: first_date_of_wk, first_date_of_promo_wk(in_dt)
    """
    in_date = datetime.strptime(in_dt, '%Y-%m-%d')
    
    ## get year, weekday number
    in_yr   = int(in_date.strftime('%y'))  ## return last 2 digit of year ex: 22
    in_year = int(in_date.strftime('%Y'))
    
    ## create next year value    
    in_nxyr   = int(in_yr) + 1
    in_nxyear = int(in_year) + 1
    
    ## Step 0: Prepre first date of this year and nextyear
    ## For Lotus first week of year is week with 30th Dec 
    ## First date promo week - first Thursday of normal fis week.
    
    bkyr_1date  = first_date_of_wk(str(in_year -2) + '-' + '12-30')
    bkyr_1promo = bkyr_1date + timedelta(days = 3)  ## + 3 days from Monday = Thursday
        
    inyr_1date  = first_date_of_wk(str(in_year -1) + '-' + '12-30')
    inyr_1promo = inyr_1date + timedelta(days = 3)  ## + 3 days from Monday = Thursday
    
    nxyr_1date  = first_date_of_wk(str(in_nxyear -1) + '-' + '12-30')
    nxyr_1promo = nxyr_1date + timedelta(days = 3)
        
    ## Step 1 : Check which year to get first date of year
    ## Step 2 : Get first date of year and diff between current date  THen get week
    
    if in_date < inyr_1promo:  ## use back year week
    
        fdate_yr = bkyr_1promo
        
        dt_delta = in_date - fdate_yr
        
        ## get number of days different from current date        
        dt_diff  = dt_delta.days
        
        ## Convert number of days to #weeks of year (+ 7 to ceiling)
        wk    = int((dt_diff + 7)/7)
        
        ## multiply 100 to shift bit + number of week
        
        fiswk = ( (in_year-1) * 100)+ wk
        
    elif in_date < nxyr_1promo :
    
        fdate_yr = inyr_1promo
        
        dt_delta = in_date - fdate_yr
        
        ## get number of days different from current date
        dt_diff  = dt_delta.days
        
        ## Convert number of days to #weeks of year (+ 7 to ceiling)
        wk       = int((dt_diff + 7)/7)
        
        ## multiply 100 to shift bit + number of week
        
        promo_wk = (in_year * 100)+ wk
        
    else:   ## in date is in next year promo_week, set as first week
        fdate_yr = nxyr_1promo
        ## return first week of year
        promo_wk    = (int(in_nxyear) * 100) + 1 
    ## end if
    
    return promo_wk

## end def


def f_date_of_promo_wk(in_promo_wk):
    """
    input parameter = in_wk is week_id eg: 202203
    prerequsit Function :
    first_date_of_wk, first_date_of_promo_wk(in_date)
    return Date value
    """
    # Step : get year, week of year using mod
    wk = in_promo_wk % 100
    
    ## 4 digit year
    in_yr     = (in_promo_wk - wk)/100
    
    diff_days = (wk-1) * 7  ## diff date 
    
    ## step2 get date from first date of year (in first week)
    
    fdate_year = first_date_of_wk(str(int(in_yr)-1) + '-12-31')
    
    fdate_promo = fdate_year + timedelta(days=3)  ## Plus 3 to be Thursday
    
    dt_wk    = fdate_promo + timedelta(days=diff_days)
    
    return dt_wk

## End def

def promo_week_cal (in_promo_wk, n) :

    """
    Description : Get week apart from "in_wk" by "n" (weeks)
    Input parameter : 2
    in_wk = week number format yyyywk
    n     = number of weeks apart from "in_wk' - exclusive in_wk (if minus then week before)
    Prerequsit function:
    first_date_of_wk, wk_of_year_promo_ls, f_date_of_promo_wk
    
    """
    
    in_dt   = f_date_of_promo_wk(in_promo_wk)
    
    diff_dy = n * 7
    
    out_dt  = in_dt + timedelta(days=diff_dy)
    out_wk  = wk_of_year_promo_ls(out_dt.strftime('%Y-%m-%d'))
    
    return out_wk

## End def


def get_period_wk_col_nm(cmp) -> str:
    """Column name for period week use for kpi calculation"""
    if cmp.wk_type in ["promo_week", "promo_wk"]:
        period_wk_col_nm = "period_promo_wk"
    elif cmp.wk_type in ["promozone"]:
        period_wk_col_nm = "period_promo_wk"
    else:
        period_wk_col_nm = "period_fis_wk"

    return period_wk_col_nm


def get_period_cust_mv_wk_col_nm(cmp) -> str:
    """Column name for period week use for Customer movement
    - Promozone eval have use special column for "period_promo_mv_wk"
    """
    if cmp.mv_week_type in ["promo_week", "promo_wk"]:
        period_wk_col_nm = "period_promo_wk"
    elif cmp.mv_week_type in ["promozone"]:
        period_wk_col_nm = "period_promo_mv_wk"
    else:
        period_wk_col_nm = "period_fis_wk"

    return period_wk_col_nm


def get_wk_id_col_nm(cmp) -> str:
    """Column name for period week identification"""
    if cmp.wk_type in ["promo_week", "promo_wk"]:
        wk_id_col_nm = "promoweek_id"
    elif cmp.wk_type in ["promozone"]:
        wk_id_col_nm = "promoweek_id"
    else:
        wk_id_col_nm = "week_id"

    return wk_id_col_nm
