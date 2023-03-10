from pyspark.sql import SparkSession, Window
import pyspark
from operator import add
from functools import reduce
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from omegaconf import OmegaConf

from .utils import *
from .constmap import *

source = OmegaConf.load('config/source.yaml')

def ori_mature_remaining_mature(frame):
    """
    (I-H) / 365
    (I-J) / 365
    
    IF(D2="N",0,IF(ISERROR(E2/SUMIFS(E:E,B:B,B2,D:D,"Y")),0,E2/SUMIFS(E:E,B:B,B2,D:D,"Y")))
    """
    frame = frame.withColumn("COLL_ORIGINAL_MATURITY", datediff(col("COLL_MATURITY_DATE"), col("COLL_ORIGINAL_DATE")) / 365.0)

    frame = frame.withColumn("COLL_REMAINING_MATURITY", datediff(col("COLL_MATURITY_DATE"), col("REPORTING_DATE")) / 365.0)

    return frame

def coll_remaining_origin_maturity(frame):
    bucket_mapping = make_spark_mapping('9. REG TABLE', 'COLL MATURITY BUCKETS')
    bucket_dct = make_lookup(bucket_mapping, 'MATURITY_BUCKET', 'MAX_MATURITY')
    bucket_dct = {k: int(v) for k, v in bucket_dct.items() if k!='NaN'}

    ####
    frame = frame.withColumn('COLL_ORIGINAL_MATURITY_BUCKET',\
        find_ltv(bucket_dct, 'NA')(col('COLL_ORIGINAL_MATURITY')))\
        .withColumn('COLL_REMAINING_MATURITY_BUCKET',\
            find_ltv(bucket_dct, 'NA')(col('COLL_REMAINING_MATURITY')))
    return frame


# =IF(K2="","LT7",VLOOKUP(K2,$'9. REG TABLE'.D:E,2,0))
def rating_cd(frame):
    rating_table = make_spark_mapping('9. REG TABLE', 'RATING TABLE')
    
    key1 = 'COLL_SEC_ISSUER_RATING'
    key2 = 'CUSTOMER_RATING'
    cols = ['RATING_CD']
    frame = join_frame(frame, rating_table, key1, key2, cols)
    
    frame = frame.withColumn("COLL_RATING_CD", \
        when(col('COLL_SEC_ISSUER_RATING').isNull(), "LT7") \
            .otherwise(col('RATING_CD'))).drop('RATING_CD')
    
    return frame

def crm_cd_eligible_crm(frame):
    rating_table = make_spark_mapping('7. REG TABLE CAL', 'collateral_mapping')
    concat_cols = ['COLL_TYPE', 'COLL_RATING_CD', 'COLL_ORIGINAL_MATURITY_BUCKET']
    lst = ["CRM_DEBT_OTH_BANK","CRM_DEBT_OTH_CORP","CRM_VAL_PAPER_FOR_GOV","CRM_VAL_PAPER_GOV_SBV","CRM_DEBT_PSE"]


    frame = frame.withColumn("concat", when(col('COLL_TYPE').isin(lst), concat_col()(struct([col(x) for x in concat_cols]))).otherwise(\
        concat_col()(struct([col(x) for x in concat_cols[:-1]]))))

    key1, key2 = 'concat', 'COLL_TYPE'
    cols = ['CRM_CD', 'ELIGIBLE_CRM']

    frame = join_frame(frame, rating_table, key1, key2, cols).drop('concat')
    return frame

def haircut_cd(frame):
    haircut_mapping = make_spark_mapping('7. REG TABLE CAL', 'haircut')
    haircut_mapping = haircut_mapping.where(~col('HAIRCUT_CD').isNull())

    ###
    lst = ['CRM_DEBT_OTH_BANK', 'CRM_DEBT_OTH_CORP', 'CRM_DEBT_PSE', 'CRM_VAL_PAPER_FOR_GOV', 'CRM_VAL_PAPER_GOV_SBV']
    concat_cols = ['COLL_TYPE', 'CRM_CD', 'COLL_REMAINING_MATURITY_BUCKET', 'COLL_RATING_CD']
    frame =  frame.withColumn('concat',\
        when(col('COLL_TYPE').isin(lst), concat_col()(struct([col(x) for x in concat_cols]))).otherwise(concat_col()(struct([col(x) for x in concat_cols[:3]])))
        )

    frame =  frame.join(haircut_mapping, t['concat'] == haircut_mapping['Concatenated column'], how = 'left').select(t['*'], haircut_mapping['HAIRCUT_CD'].alias('tmp'))
    frame =  frame.fillna({'tmp': 'N/A'})

    frame =  frame.withColumn('HAIRCUT_CD',\
        when(col('ELIGIBLE_CRM')== 'NO', 'N/A').otherwise(col('tmp'))).drop('tmp', 'concat')
    return frame

def haircut_percent(frame):
    # IF(S2="N/A",100%,VLOOKUP(S2,$'9. REG TABLE'.$M$8:$N$22,2,0))
    haircut_table = make_spark_mapping('9. REG TABLE', 'HAIRCUT TABLE')
    
    key1 = 'HAIRCUT_CD'
    key2 = 'HAIRCUT_CD'
    cols = ['HAIRCUT%']
    frame = join_frame(frame, haircut_table, key1, key2, cols)
    frame = frame.withColumnRenamed('HAIRCUT%', 'HAIRCUTT')
    
    frame = frame.withColumn("HAIRCUT%", \
        when(col('HAIRCUT_CD')=='N/A', "100%") \
            .otherwise(col('HAIRCUTT')))
    
    return frame

def crm_eligible(frame):
    """=IF(R2="NO",0,IF(ISERROR(F2/SUMIFS(F:F,B:B,B2,R:R,"YES")),0,F2/SUMIFS(F:F,B:B,B2,R:R,"YES")))"""
     
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))

    frame = frame.withColumn('sum',
            when(col('ELIGIBLE_CRM') == 'YES', 0).otherwise(
                sum(col('COLL_VALUE')).over(window_spec)
            )
        ).withColumn("COLL_CRM_ELIGIBLE %",
            when(col("ELIGIBLE_CRM") == "NO", 0)\
                .otherwise(
                    when(col("sum") == 0, 0)\
                    .otherwise(when(is_error(col("COLL_VALUE") / col("sum"))==1, 0)\
                    .otherwise(col("COLL_VALUE") / col("sum")))
                )       
        ).drop('sum')
    
    return frame

def final_col(frame):
    """=SUMIFS($'6. EXPOSURE'.AQ:AQ,$'6. EXPOSURE'.B:B,$'3. COLLATERAL'.B2)*$'3. COLLATERAL'.U2*(1-T2)
    """
    exposure = read_excel('/content/drive/MyDrive/Excel2spark/input_test/EXPOSURE.xlsx')
    
    key1="CUSTOMER_ID"
    key2="CUSTOMER_ID"
    cols = ['ADJUSTED_COLL_MATURITY']
    # Join 2 table
    frame = join_frame(frame, exposure, key1, key2, cols)
    
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))
    frame = frame.withColumn("sum", 
                            sum(col('ADJUSTED_COLL_MATURITY')).over(window_spec)) \
                .withColumn("ALLOCATED_COLLATERAL_AFTER_MT_MISMATCH_AND_HAIRCUT", col("sum")*col("COLL_CRM_ELIGIBLE %") * (1-col('HAIRCUT%'))).drop('sum')
    
    return frame