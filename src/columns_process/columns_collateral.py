from pyspark.sql import SparkSession, Window
import pyspark
from operator import add
from functools import reduce
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from omegaconf import OmegaConf

from src.utils import *
from src.constmap import *


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
    """
    N: =IF(L2<=$'9. REG TABLE'.$U$9,$'9. REG TABLE'.$V$9,IF(L2<=$'9. REG TABLE'.$U$10,$'9. REG TABLE'.$V$10,IF(L2<=$'9. REG TABLE'.$U$11,$'9. REG TABLE'.$V$11,IF(L2<=$'9. REG TABLE'.$U$12,$'9. REG TABLE'.$V$12,IF(L2<=$'9. REG TABLE'.$U$13,$'9. REG TABLE'.$V$13,"NA")))))
    
    O: =IF(M2<=$'9. REG TABLE'.$U$9,$'9. REG TABLE'.$V$9,IF(M2<=$'9. REG TABLE'.$U$10,$'9. REG TABLE'.$V$10,IF(M2<=$'9. REG TABLE'.$U$11,$'9. REG TABLE'.$V$11,IF(M2<=$'9. REG TABLE'.$U$12,$'9. REG TABLE'.$V$12,IF(M2<=$'9. REG TABLE'.$U$13,$'9. REG TABLE'.$V$13,"NA")))))
    """
    bucket_mapping = make_spark_mapping('9. REG TABLE', 'COLL MATURITY BUCKETS')
    bucket_dct = make_lookup(bucket_mapping, 'MATURITY_BUCKET', 'MAX_MATURITY')
    bucket_dct = {k: int(v) for k, v in bucket_dct.items() if k!='NaN'}

    ####
    frame = frame.withColumn('COLL_ORIGINAL_MATURITY_BUCKET',\
        find_ltv(bucket_dct, 'NA')(col('COLL_ORIGINAL_MATURITY')))\
        .withColumn('COLL_REMAINING_MATURITY_BUCKET',\
            find_ltv(bucket_dct, 'NA')(col('COLL_REMAINING_MATURITY')))
    return frame


def rating_cd(frame):
    """
    P: =IF(K2="","LT7",VLOOKUP(K2,$'9. REG TABLE'.D:E,2,0))
    """
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
    """
    Q: =IF(OR(D2="CRM_DEBT_OTH_BANK",D2="CRM_DEBT_OTH_CORP",D2="CRM_VAL_PAPER_FOR_GOV",D2="CRM_VAL_PAPER_GOV_SBV",D2="CRM_DEBT_PSE"),
            VLOOKUP(D2&P2&N2,$'7. REG TABLE CAL'.J:L,2,FALSE()),
        ELSE:
            VLOOKUP(D2&P2,$'7. REG TABLE CAL'.J:L,2,FALSE()))
    
    R: =IF(OR(D2="CRM_DEBT_OTH_BANK",D2="CRM_DEBT_OTH_CORP",D2="CRM_VAL_PAPER_FOR_GOV",D2="CRM_VAL_PAPER_GOV_SBV",D2="CRM_DEBT_PSE"),
            VLOOKUP(D2&P2&N2,$'7. REG TABLE CAL'.J:L,3,FALSE()),
        ELSE:
            VLOOKUP(D2&P2,$'7. REG TABLE CAL'.J:L,3,FALSE()))
    """
    rating_table = make_spark_mapping('7. REG TABLE CAL', 'collateral_mapping')
        
    frame = frame.withColumn("concat_value3", concat(col("COLL_TYPE"), col("COLL_RATING_CD"), col('COLL_ORIGINAL_MATURITY_BUCKET')))
    frame = frame.withColumn("concat_value2", concat(col("COLL_TYPE"), col("COLL_RATING_CD")))

    key1 = 'concat_value3'
    key1_1 = 'concat_value2'
    key2 = 'COLL_TYPE'
    cols = ['CRM_CD', 'ELIGIBLE_CRM']


    frame1 = frame.join(rating_table, frame[key1] == rating_table[key2], how = 'left')\
            .select(frame['CUSTOMER_ID'], col('CRM_CD'), col('ELIGIBLE_CRM')).withColumnRenamed("CRM_CD", "CRM_CD1").withColumnRenamed("ELIGIBLE_CRM", "ELIGIBLE_CRM1")

    frame2 = frame.join(rating_table, frame[key1_1] == rating_table[key2], how = 'left')\
            .select(frame['CUSTOMER_ID'], col('CRM_CD'), col('ELIGIBLE_CRM')).withColumnRenamed("CRM_CD", "CRM_CD2").withColumnRenamed("ELIGIBLE_CRM", "ELIGIBLE_CRM2")

    frame = frame.join(frame1, on='CUSTOMER_ID', how='left').join(frame2, on='CUSTOMER_ID', how='left')

    frame = frame.withColumn('CRM_CD', 
                    when((col('COLL_TYPE') == 'CRM_DEBT_OTH_BANK') | (col('COLL_TYPE') == 'CRM_DEBT_OTH_CORP') | 
                        (col('COLL_TYPE') == 'CRM_VAL_PAPER_FOR_GOV') | (col('COLL_TYPE') == 'CRM_VAL_PAPER_GOV_SBV') | 
                        (col('COLL_TYPE') == 'CRM_DEBT_PSE'), frame1['CRM_CD1'])
                    .otherwise(frame2['CRM_CD2']))

    frame = frame.withColumn('ELIGIBLE_CRM', 
                    when((col('COLL_TYPE') == 'CRM_DEBT_OTH_BANK') | (col('COLL_TYPE') == 'CRM_DEBT_OTH_CORP') | 
                        (col('COLL_TYPE') == 'CRM_VAL_PAPER_FOR_GOV') | (col('COLL_TYPE') == 'CRM_VAL_PAPER_GOV_SBV') | 
                        (col('COLL_TYPE') == 'CRM_DEBT_PSE'), frame1['ELIGIBLE_CRM1'])
                    .otherwise(frame2['ELIGIBLE_CRM2']))
    frame = frame.drop("concat_value3", "concat_value2", "CRM_CD1", "CRM_CD2", "ELIGIBLE_CRM1", "ELIGIBLE_CRM2")
    
    return frame

def haircut_cd(frame):
    """
    S=IF(R2="NO",
        "N/A",
    ELSE:
        INDEX($'7. REG TABLE CAL'.$T$10:$U$190,
            MATCH(
                IF(OR(D2="CRM_DEBT_OTH_BANK",D2="CRM_DEBT_OTH_CORP",D2="CRM_DEBT_PSE",D2="CRM_VAL_PAPER_FOR_GOV",D2="CRM_VAL_PAPER_GOV_SBV"),
                        TRIM(CONCATENATE(D2,Q2,O2,P2)),
                    ELSE:
                        TRIM(CONCATENATE(D2,Q2))),$'7. REG TABLE CAL'.$T$10:$T$190,0),2))
    """
    haircut_mapping = make_spark_mapping('7. REG TABLE CAL', 'haircut')
    haircut_mapping = haircut_mapping.where(~col('HAIRCUT_CD').isNull())

    ###
    lst = ['CRM_DEBT_OTH_BANK', 'CRM_DEBT_OTH_CORP', 'CRM_DEBT_PSE', 'CRM_VAL_PAPER_FOR_GOV', 'CRM_VAL_PAPER_GOV_SBV']
    concat_cols = ['COLL_TYPE', 'CRM_CD', 'COLL_REMAINING_MATURITY_BUCKET', 'COLL_RATING_CD']
    frame =  frame.withColumn('concat',\
        when(col('COLL_TYPE').isin(lst), concat_col()(struct([col(x) for x in concat_cols]))).otherwise(concat_col()(struct([col(x) for x in concat_cols[:2]])))
        )

    frame =  frame.join(haircut_mapping, frame['concat'] == haircut_mapping['Concatenated column'], how = 'left').select(frame['*'], haircut_mapping['HAIRCUT_CD'].alias('tmp'))
    frame =  frame.fillna({'tmp': 'N/A'})

    frame =  frame.withColumn('HAIRCUT_CD',\
        when(col('ELIGIBLE_CRM')== 'NO', 'N/A').otherwise(col('tmp'))).drop('tmp', 'concat', 'HAIRCUTT')
    return frame

def haircut_percent(frame):
    """
    T: =IF(S2="N/A",100%,VLOOKUP(S2,$'9. REG TABLE'.$M$8:$N$22,2,0))
    """
    haircut_table = make_spark_mapping('9. REG TABLE', 'HAIRCUT TABLE')
    
    key1 = 'HAIRCUT_CD'
    key2 = 'HAIRCUT_CD'
    cols = ['HAIRCUT%']
    frame = join_frame(frame, haircut_table, key1, key2, cols)
    frame = frame.withColumnRenamed('HAIRCUT%', 'HAIRCUTT')
    
    frame = frame.withColumn("HAIRCUT%", \
        when(col('HAIRCUT_CD')=='N/A', 1) \
            .otherwise(col('HAIRCUTT')))
    
    return frame

def crm_eligible(frame):
    """
    U: =IF(R2="NO",
        0,
    ELSE:
        IF(ISERROR(F2/SUMIFS(F:F,B:B,B2,R:R,"YES"))
            0,
        ELSE:
            F2/SUMIFS(F:F,B:B,B2,R:R,"YES")))
    """
    window_spec = Window.partitionBy(col("CUSTOMER_ID"))

    frame = frame.withColumn('sum',
            when(col('ELIGIBLE_CRM') == 'YES', sum(col('COLL_VALUE')).over(window_spec)
            )
        ).withColumn("COLL_CRM_ELIGIBLE %",
            when(col("ELIGIBLE_CRM") == "NO", 0)\
                .otherwise(
                    when(col("sum") == 0, 0)\
                    .otherwise(when(is_error(col("COLL_VALUE") / col("sum"))==1, 0)\
                    .otherwise(col("COLL_VALUE") / col("sum")))
                )       
        )
        # .withColumn("COLL_CRM_ELIGIBLE %",\
        #              concat((col("tmp")*100), lit("%"))).drop("sum", "tmp")
        
    return frame

def final_col(frame, exposure):
    """
    V: =SUMIFS($'6. EXPOSURE'.AQ:AQ,$'6. EXPOSURE'.B:B,$'3. COLLATERAL'.B2)*$'3. COLLATERAL'.U2*(1-T2)
    """
        
    key1="CUSTOMER_ID"
    key2="CUSTOMER_ID"
    cols = ['ADJUSTED_COLL_MATURITY']
    # Join 2 table
    frame = join_frame(frame, exposure, key1, key2, cols)
    frame = frame.fillna({'ADJUSTED_COLL_MATURITY': 0})

    window_spec = Window.partitionBy(col("CUSTOMER_ID"))
    frame = frame.withColumn("sum", 
                            sum(col('ADJUSTED_COLL_MATURITY')).over(window_spec)) \
                .withColumn("ALLOCATED_COLLATERAL_AFTER_MT_MISMATCH_AND_HAIRCUT", \
                            col("sum")*col("COLL_CRM_ELIGIBLE %") * (1-col('HAIRCUT%'))).drop("ADJUSTED_COLL_MATURITY") \
                .withColumn("HAIRCUT%", concat(col("HAIRCUT%") * 100, lit("%"))) \
                .withColumn("COLL_CRM_ELIGIBLE %", concat(col("COLL_CRM_ELIGIBLE %") * 100, lit("%"))) \
                .drop("sum", "HAIRCUTT")

    frame = frame.where(~col("CUSTOMER_ID").isNull())
    
    return frame