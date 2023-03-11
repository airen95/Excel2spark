from pyspark.sql import SparkSession, Window
import pyspark
from operator import add
from functools import reduce
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from omegaconf import OmegaConf

from src.utils import *

source = OmegaConf.load('config/source.yaml')

def original_maturity(frame):
    frame = frame.withColumn('ORIGINAL_MATURITY', \
    when(col('EXPOSURE_MATURITY_DATE').isNull() & col('EXPOSURE_ORIGINAL_DATE').isNull(), 0)\
        .otherwise(datediff(col("EXPOSURE_MATURITY_DATE"),col("EXPOSURE_ORIGINAL_DATE"))/365))

    return frame

def residual_maturity(frame):
    frame = frame.withColumn('RESIDUAL_MATURITY', \
    when(col('EXPOSURE_MATURITY_DATE').isNull() & col('REPORTING_DATE').isNull(), 0)\
        .otherwise(datediff(col("EXPOSURE_MATURITY_DATE"),col("REPORTING_DATE"))/365))

    return frame



def cpty_type_cpty_sub_type_borrower_income_source_curr(frame, customer):
    # customer = read_excel(source.data_path['customer'])

    key1 = 'CUSTOMER_ID'
    key2 = key1
    cols = ['BORROWER_INCOME_SOURCE_CURR', 'CPTY_TYPE8', 'CPTY_SUB_TYPE9']

    frame = join_frame(frame, customer, key1, key2, cols)

    rename = {'CPTY_TYPE8': 'CPTY_TYPE',
    'CPTY_SUB_TYPE9': 'CPTY_SUB_TYPE'}

    for key, value in rename.items():
        frame = frame.withColumnRenamed(key, value)

    return frame

def loan_retail_exposure_secured_by_real_estate(frame):
    condition_in = ['CLASS_OTH_RETAIL', 'CLASS_REG_RETAIL', 'CLASS_SME']
    condition_out = ['Income CRE', 'General CRE', 'General RRE', 'ADC']
    sum1 = ['OUTSTANDING_AMT_LCY / UTILIZED LIMIT', 'ACCRUED_INTEREST_LCY', 'OUTSTANDING_AMT / UTILIZED LIMIT']
    # sum2 = ['OUTSTANDING_AMT_LCY / UTILIZED LIMIT', 'ACCRUED_INTEREST_LCY']
    frame = frame.withColumn('LOAN_RETAIL_EXPOSURE NOT SECURED BY REAL ESTATE',\
        when((col('ASSET_CLASS_FINAL').isin(condition_in)) & ((~col('ASSET_CLASS_MANUAL').isin(condition_out))| (col('ASSET_CLASS_MANUAL').isNull())),\
            when(col('OUTSTANDING_AMT / UTILIZED LIMIT')== 'NA', reduce(add, [col(x) for x in sum1[:-1]])).otherwise(reduce(add, [col(x) for x in sum1])))\
            .otherwise('N/A')
    
        )
    return frame


def reg_retail_8b_flag(frame):
    retail_mapping = make_spark_mapping('9. REG TABLE', 'RETAIL QUALIFYING CRITERIA')
    value = int(retail_mapping.select('VALUE').collect()[1][0])

    condition_in = ['SME_TT41', 'RETAIL']
    frame = frame.withColumn('sum_X', \
        when(col('LOAN_RETAIL_EXPOSURE NOT SECURED BY REAL ESTATE') == 'N/A', 'N/A').otherwise(
            sum(col('LOAN_RETAIL_EXPOSURE NOT SECURED BY REAL ESTATE')).over(Window.partitionBy(col("CUSTOMER_ID")))
        )).withColumn('REG_RETAIL_8B FLAG',\
        when(col('sum_X') == 'N/A', 'N/A').otherwise(
            when((col('CPTY_SUB_TYPE').isin(condition_in)) &
                (col('sum_X').cast('double')<= value), "REGULATORY RETAIL").otherwise('N/A'))
        ).drop('sum_X')

    return frame



def transactor_flag(frame, od, cc):
    # od = read_excel(source.data_path['od'])
    # cc  = read_excel(source.data_path['cc'])

    frame = frame.join(od, frame.EXPOSURE_ID == od['Số TK vay'], how = 'left').select(frame['*'], od['Transactor flag'].alias('flag1'))
    frame = frame.join(cc, frame.EXPOSURE_ID == cc['Số TK'], how = 'left').select(frame['*'], cc['Transactor flag'].alias('flag2'))

    frame = frame.withColumn('TRANSACTOR_FLAG', coalesce('flag1', 'flag2')).drop('flag1', 'flag2')
    frame = frame.fillna({"TRANSACTOR_FLAG": "N"})
    return frame

def specific_provision(frame):
    # condition_in = ['0', 'NA', ""]
    frame = frame.withColumn('Specific Provision %', \
        when((is_error(col('SPECIFIC_PROVISION')) ==1) | (col('SPECIFIC_PROVISION').isNull()) | (is_error(col('OUTSTANDING_AMT_LCY / UTILIZED LIMIT'))==1), 0).\
            otherwise((col('SPECIFIC_PROVISION')/col('OUTSTANDING_AMT_LCY / UTILIZED LIMIT')).cast('double')))
    return frame

def regulatory_retail_flag_b4(frame):
    retail_mapping = make_spark_mapping('9. REG TABLE', 'RETAIL QUALIFYING CRITERIA')
    value = float(retail_mapping.select('VALUE').collect()[2][0])

    frame = frame.withColumn('sum_X', \
    when(col('PAST_DUE_FLAG') != 'P', 'N/A').otherwise(
        sum(col('LOAN_RETAIL_EXPOSURE NOT SECURED BY REAL ESTATE')).over(Window.partitionBy(col("CUSTOMER_ID")))
    ))\
    .withColumn('sum_Y', \
        when((col('PAST_DUE_FLAG') != 'P') | (col('REG_RETAIL_8B FLAG') != "REGULATORY RETAIL"), 'N/A').otherwise(
            sum(col('LOAN_RETAIL_EXPOSURE NOT SECURED BY REAL ESTATE')).over(Window.partitionBy(col("CUSTOMER_ID")))
        ))\
    .withColumn('devide', when((~col('sum_X').isNull()) & (~col('sum_Y').isNull()), col('sum_X')/col('sum_Y')).\
        otherwise('N/A'))\
    .withColumn('REGULATORY_RETAIL_FLAG_B4',
        when(col('devide') <= value, \
            when(col('CPTY_SUB_TYPE') == 'SME_TT41', 'CLASS_SME_REGULATORY_RETAIL').otherwise('CLASS_REG_RETAIL')
            ).otherwise('N/A')
        
    ).drop('sum_X', 'sum_Y', 'devide')

    return frame


def prd_cd_prd_sub_cd(frame):
    product_mapping = make_spark_mapping('7. REG TABLE CAL', 'product_mapping')

    condition_in = ["General CRE", "Income CRE", "General RRE", "Income RRE"]
    condition_in1 = ["CLASS_VN_SOV", "CLASS_DATC_VAMC"]



    frame = frame.withColumn('tmp',\
            when(col('ASSET_CLASS_MANUAL').isin(condition_in), col('ASSET_CLASS_MANUAL')).otherwise(\
                when(col('REGULATORY_RETAIL_FLAG_B4') == 'CLASS_REG_RETAIL', 'REGULATORY RETAIL').otherwise(\
                    when(col('REGULATORY_RETAIL_FLAG_B4') == 'CLASS_SME_REGULATORY_RETAIL', \
                        concat(col('REGULATORY_RETAIL_FLAG_B4'),col('EXPOSURE_SUB_TYPE_CODE'), col('ASSET_CLASS_MANUAL'))).otherwise(
                            when(col('ASSET_CLASS_FINAL').isin(condition_in1),\
                                concat(col('ASSET_CLASS_FINAL'), col('ERA_PRODUCT_TYPE'))).otherwise(
                                    concat(col('ASSET_CLASS_FINAL'), col('EXPOSURE_SUB_TYPE_CODE'), col('ASSET_CLASS_MANUAL'))
                                )
                        )
                )

            )
        
        )
    frame = frame.withColumn('tmp', trim(col('tmp')))

    frame = frame.join(product_mapping, frame.tmp == product_mapping['Concatenated column'], how = 'left')\
        .select(frame['*'], product_mapping['PRODUCT_TYPE'].alias('PRD_CD'), product_mapping['PRODUCT_SUB_TYPE'].alias('PRD_SUB_CD'))

    frame = frame.withColumn('PRD_CD', when(col('tmp') == 'REGULATORY RETAIL', 'REGULATORY RETAIL').otherwise(col('PRD_CD')))
    frame = frame.withColumn('PRD_SUB_CD', when(col('tmp') == 'REGULATORY RETAIL', 'REGULATORY RETAIL').otherwise(col('PRD_SUB_CD')))

    frame = frame.drop('tmp')

    return frame



def asset_class_asset_sub_class(frame):
    assess_mapping = make_spark_mapping('7. REG TABLE CAL', 'assess_mapping')

    key1, key2 = 'tmp1', 'Concatenated column'
    cols = ['ASSET_CLASS', 'ASSET_SUB_CLASS']

    frame = frame.withColumn('tmp1',\
    when((col('PAST_DUE_FLAG') == 'D') & (col('PRD_SUB_CD') == 'RRE_GEN'),\
        concat(col('PAST_DUE_FLAG'), col('PRD_CD'), col('PRD_SUB_CD'))).otherwise(\
            when(col('PAST_DUE_FLAG') == 'D', 'BAD_DEBT').otherwise(\
                when((col('PRD_CD') == 'REGULATORY RETAIL') & (col('TRANSACTOR_FLAG') == 'Y'), 'RETAIL').otherwise(\
                    concat(col('PRD_CD'), col('PRD_SUB_CD')))
            )
        ))

    # frame = frame.join(asset_mapping, frame.tmp == asset_mapping['Concatenated column'], how = 'left')\
    #     .select(frame['*'], asset_mapping['ASSET_CLASS'], asset_mapping['ASSET_SUB_CLASS'])
    frame = join_frame(frame, assess_mapping, key1, key2, cols)

    frame = frame.withColumn('ASSET_CLASS', when(col('tmp1').isin('BAD_DEBT', 'RETAIL'), col('tmp1')).otherwise(col('ASSET_CLASS')))
    frame = frame.withColumn('ASSET_SUB_CLASS', when(col('tmp1').isin('BAD_DEBT', 'RETAIL'), col('tmp1')).otherwise(col('ASSET_SUB_CLASS')))

    frame = frame.drop('tmp1')

    return frame

def specific_provision_bucket(frame):
    provision_mapping = make_spark_mapping('9. REG TABLE', 'SPECIFIC PROVISIONS BUCKETS')
    provision_mapping = provision_mapping.filter(col('MIN_PROVISION')!='NaN')

    value = float(provision_mapping.select('MAX_PROVISION').collect()[0][0])
    value1 = float(provision_mapping.select('MAX_PROVISION').collect()[1][0])

    frame = frame.withColumn('Specific Provision Bucket',\
    when(col('Specific Provision %') < value, provision_mapping.select('MATURITY_BUCKET').collect()[0][0]).otherwise(\
        when(col('Specific Provision %')< value1, provision_mapping.select('MATURITY_BUCKET').collect()[1][0]).otherwise(\
            "CHECK")       
        ))
    return frame

def cust_rating(frame):
    rating_table_mapping = make_spark_mapping('7. REG TABLE CAL', 'RATING TABLE MAPPING')
    frame = frame.withColumn('tmp2',\
        when(col('ASSET_SUB_CLASS') == 'COVERED BOND RATED',\
            when(~concat(col('RATING_AGENCY CODE'), col('SPECIFIC RATING_COVERED BOND')).isin(['NANA', '']),\
                concat(col('RATING_AGENCY CODE'), col('SPECIFIC RATING_COVERED BOND'))).otherwise('CHECK AGAIN')).otherwise(\
                    col('CUSTOMER_ID')                
                    )   
        )
    frame = frame.join(rating_table_mapping, (frame['tmp2'] == rating_table_mapping['Concatenated column']) &\
        (frame['tmp2'] == "COVERED BOND RATED"), how = 'left').select(frame['*'], \
            rating_table_mapping['RATING_CD'].alias('CUST_RATING1'))
    frame = frame.withColumn('CUST_RATING1', when(col('tmp2') == 'CHECK AGAIN', 'CHECK_AGAIN').otherwise(col('CUST_RATING1')))

    customer = read_excel('/home/dieule/Downloads/input_test/CUSTOMER.xlsx')
    frame = frame.join(customer, (frame['tmp2'] == customer['CUSTOMER_ID'])&(frame['tmp2'] != "COVERED BOND RATED"), how = 'left')\
        .select(frame['*'], customer['CUST_RATING_CD'].alias('CUST_RATING2'))

    frame = frame.withColumn('CUST_RATING', coalesce(frame['CUST_RATING1'], frame['CUST_RATING2'], lit('LT7')))\
        .drop('CUST_RATING1', 'CUST_RATING2', 'tmp2')

    return frame


def exposure_percent(frame):
    col2sum = ['OUTSTANDING_AMT_LCY / UTILIZED LIMIT', 'ACCRUED_INTEREST_LCY', 'OUTSTANDING_FEES_PENALITIES_LCY']
    frame = frame.withColumn('sum1', reduce(add, [replace_null(col(x), lit(0)) for x in col2sum]))\
        .withColumn('groupH', sum(replace_null(col(col2sum[0]), lit(0))).over(Window.partitionBy(col('CUSTOMER_ID'))))\
        .withColumn('groupI', sum(replace_null(col(col2sum[1]), lit(0))).over(Window.partitionBy(col('CUSTOMER_ID'))))\
        .withColumn('groupJ', sum(replace_null(col(col2sum[2]), lit(0))).over(Window.partitionBy(col('CUSTOMER_ID'))))\
        .withColumn('EXPOSURE %', col('sum1')/reduce(add, (col('sum1'), col('groupH'), col('groupI'), col('groupJ'))))

    frame = frame.withColumn('EXPOSURE %', when(col('EXPOSURE %').isNull(), 0).otherwise(col('EXPOSURE %'))).drop('sum1', 'groupH', 'groupI', 'groupJ')
    return frame

def ltv(frame, collateral):
    # collateral = read_excel(source.data_path['collateral'])
    key1, key2 = 'CUSTOMER_ID', 'CUSTOMER_ID'
    cols = ['COLL_TYPE','COLL_CCY', 'ELIGIBLE_REAL ESTATE','COLL_VALUE','ELIGIBLE_CRM','COLL_ORIGINAL_MATURITY','COLL_REMAINING_MATURITY']#,\
        # 'COLL_CCY', 'ALLOCATED_COLLATERAL_AFTER_MT_MISMATCH_AND_HAIRCUT']

    frame = join_frame(frame, collateral, key1, key2, cols)
    condition = ["CRE_GEN","CRE_INC","RRE_GEN","RRE_INC"]

    frame = frame.withColumn('sum1',\
                sum(replace_null(col('OUTSTANDING_AMT_LCY / UTILIZED LIMIT'), lit(0))).over(Window.partitionBy('CUSTOMER_ID')))\
            .withColumn('sumif',\
                when((col('COLL_TYPE')!= 'CRM_RE') | (col('ELIGIBLE_REAL ESTATE')!= 'Y'), 'N/A').otherwise(\
                    sum(col('COLL_VALUE')).over(Window.partitionBy('CUSTOMER_ID'))))\
            .withColumn('devide',\
                col('sum1')/col('sumif'))\
            .withColumn('LTV', \
                when((col('ASSET_SUB_CLASS').isNull()) | (col('ASSET_SUB_CLASS').isin('N/A', 'NA', '')), 'CHECK').otherwise(\
                    when(col('EXPOSURE %') == 0, 0).otherwise(\
                        when((col('ASSET_SUB_CLASS').isNull())| (col('ASSET_SUB_CLASS').isin('', 'NA', 'N/A'))| (col('devide').isNull()), 'N/A').otherwise(
                            col('devide')
                        )
                        )
                    )
                ).drop('sum1', 'sumif', 'devide')

    return frame

def ltv_bucket(frame):
    ltv_mapping = make_spark_mapping('9. REG TABLE','LTV')
    ltv_mapping = ltv_mapping.where(ltv_mapping['LTV_CD']!='NaN')

    ltv_dct = make_lookup(ltv_mapping, 'LTV_CD', 'MAX (INCLUDING)')
    ltv_dct = {k: float(v) for k, v in ltv_dct.items()}

    frame = frame.withColumn('LTV Bucket',\
    when((is_error(col('LTV')) == 1) | (col('LTV').isNull()), 'N/A').otherwise(\
        find_ltv(ltv_dct, 'ERROR IN CONFIG')(col('LTV'))))

    return frame

def ccf(frame):
    ccf_mapping = make_spark_mapping('7. REG TABLE CAL', 'CCF MAPPING')
    ccf_mapping = ccf_mapping.where(ccf_mapping['Concatenated column']!='NaN')

    ccf_dct = make_lookup(ccf_mapping, 'Concatenated column', 'CCF_CD')
    condition_d1 = ['FC_CL', 'FC_OD', 'FC_LC', 'FC_CC']
    condition_c = ["CLASS_CORP", "CLASS_SME", "CLASS_REG_RETAIL", "CLASS_CORP_IPRE", "CLASS_CORP_CF", "CLASS_CORP_OF","CLASS_CORP_PF"]
    condition_d = ["LC"]
    condition_f = ["ISSUANCE_LC_LADING", "LC_SPECIALISED_IPRE_LC", "LC_SPECIALISED_CF_LC", "LC_SPECIALISED_PF_LC"]

    # frame = frame.withColumn('CCF', \
    #     when(col('EXPOSURE_SUB_TYPE_CODE').isin(condition_d1), vlookup(ccf_dct, col('EXPOSURE_SUB_TYPE_CODE'), 'N/A')).otherwise(\
    #         when((col('ASSET_CLASS_FINAL').isin(condition_c)) & (col('EXPOSURE_SUB_TYPE_CODE') == 'LC') & (col('ERA_PRODUCT_TYPE').isin(condition_d)),\
    #             when(col('ORIGINAL_MATURITY') < 12, vlookup(ccf_dct, concat(col('ASSET_CLASS_FINAL'), col('EXPOSURE_SUB_TYPE_CODE'), col('ERA_PRODUCT_TYPE'), '< 12 months'), 'N/A')).otherwise(\
    #                 vlookup(ccf_dct, concat(col('ASSET_CLASS_FINAL'), col('EXPOSURE_SUB_TYPE_CODE'), col('ERA_PRODUCT_TYPE'), '> 12 months'), 'N/A'))           
    #             ).otherwise(vlookup(ccf_dct, concat(col('ASSET_CLASS_FINAL'), col('EXPOSURE_SUB_TYPE_CODE'), col('ERA_PRODUCT_TYPE')), 'N/A'))))
    frame = frame.withColumn('CCF', \
        when(col('EXPOSURE_SUB_TYPE_CODE').isin(condition_d1), vlookup(ccf_dct,'N/A')(col('EXPOSURE_SUB_TYPE_CODE'))).otherwise(\
            when((col('ASSET_CLASS_FINAL').isin(condition_c)) & (col('EXPOSURE_SUB_TYPE_CODE') == 'LC') & (col('ERA_PRODUCT_TYPE').isin(condition_d)),\
                when(col('ORIGINAL_MATURITY') < 12, vlookup(ccf_dct,'N/A')(concat(col('ASSET_CLASS_FINAL'), col('EXPOSURE_SUB_TYPE_CODE'), col('ERA_PRODUCT_TYPE'), lit('< 12 months')))).otherwise(\
                vlookup(ccf_dct,'N/A')(concat(col('ASSET_CLASS_FINAL'), col('EXPOSURE_SUB_TYPE_CODE'), col('ERA_PRODUCT_TYPE'), lit('> 12 months'))) )           
                ).otherwise(vlookup(ccf_dct,'N/A')(concat(col('ASSET_CLASS_FINAL'), col('EXPOSURE_SUB_TYPE_CODE'), col('ERA_PRODUCT_TYPE')))))
    )
    return frame

def re_eligible_p60(frame):
    tmp = frame.groupBy("CUSTOMER_ID").agg((count(when(col("COLL_TYPE") == 'CRM_RE', 1)).alias("countD")),\
        (count(when((col("COLL_TYPE") == 'CRM_RE') & (col('ELIGIBLE_REAL ESTATE')=='N'), 1)).alias('countE')))
    frame = frame.join(tmp, frame['CUSTOMER_ID'] == tmp['CUSTOMER_ID'], how = 'left').select(frame['*'], tmp['countD'], tmp['countE'])

    frame = frame.withColumn('RE_ELIGIBLE P60',\
        when(frame['countD'] == 0, 'N/A').otherwise(\
            when(frame['countE'] == 0, 'Y').otherwise('N'))).drop('countD', 'countE')

    return frame

def check2concat(col1, col2, col3, col4, col5, col8, col9):
    try:
        if col1 in ae[:2]:
            return concat(col2, col1, col4)
        else:
            if col1 in ae[2:11]:
                return concat(col2, col1, col5)
            else:
                if (col1 in ae[11:13]) and col9 == 'N':
                    return concat(col2, col1, col9)
                else:
                    if (col1 in ae[12:]) and (col9 == 'N') and (col8 in w):
                        return concat(col2, col1, col9, col8)
                    else:
                        if (col1 in ae[12:]) and (col9 == 'N'):
                            return concat(col2, col1, col9, col5, col8)
                        else:
                            if col1 in ae[11:14]:
                                return concat(col2, col1, col7)
                            else:
                                if col1 == 'CRE_GEN' and col8 in w:
                                    return concat(col2, col1, col5, col7, col8)
                                else:
                                    return concat(col2, col1)
    except:
        return 'N/A'


check2concat = udf(check2concat, StringType())

def key_category(frame):
    ae = ['BAD_DEBT', 'BAD_DEBT_RRE_GEN', 'CORP_GEN', 'SME', 'DOM_CIS', 'FOR_CIS', 'MDB', 'PSE'\
        "SOVEREIGN","COVERED BOND RATED","COVERED BOND UNRATED", "CRE_INC", "RRE_INC"\
        "RRE_GEN","CRE_GEN"]
    w = ["RETAIL","SME_TT41"]

    col2cc = ['ASSET_CLASS', 'ASSET_SUB_CLASS', 'Specific Provision Bucket', 'CUST_RATING', 'CPTY_TYPE', 'CPTY_SUB_TYPE',\
        'LTV Bucket', 'RE_ELIGIBLE P60', 'CUST_RATING']
        
    frame = frame.withColumn('KEY CATEGORY', check2concat(col('ASSET_SUB_CLASS'), col('ASSET_CLASS'),\
    col('Specific Provision %'), col('Specific Provision Bucket'), col('CUST_RATING'),\
        col('CPTY_SUB_TYPE'), col('RE_ELIGIBLE P60')))

    return frame

def rw(frame):
    assess_mapping = read_excel(source.data_path['parameter'], sheet_name = '8. ASSET MAPPING')

    frame = frame.withColumn('2compare',\
            when((col('ORIGINAL_MATURITY')*12 <=3) & ((col('ASSET_CLASS') == 'FIN_INST')| (col('CPTY_SUB_TYPE') == 'FIN_INST')),1).otherwise(0))

    frame = frame.join(assess_mapping, frame['KEY CATEGORY'] == assess_mapping['Key'], how = 'left').select(frame['*'], assess_mapping['ST RISK_WEIGHT'], assess_mapping['RISK_WEIGHT'])
    frame = frame.withColumn('RW', \
            when(~col('2compare').isNull(),\
                    when(col('2compare') == 1, col('ST RISK_WEIGHT')).otherwise(\
                            when(col('2compare') == 0, col('RISK_WEIGHT')).otherwise('N/A'))
            ).otherwise('N/A')).drop('2compare')

    return frame

def rw_cc(frame):
    frame = frame.withColumn('RW_CC',\
        when(~col('BORROWER_INCOME_SOURCE_CURR').isNull(),\
            when((col('ASSET_CLASS').isin('RESIDENTIAL_REAL_ESTATE', 'RETAIL')) & (col('EXPOSURE_CURRENCY')!=col('BORROWER_INCOME_SOURCE_CURR')) & (col('RW').cast('double')*0.15 >= 0.15), 0.15).otherwise(\
                when((col('ASSET_CLASS').isin('RESIDENTIAL_REAL_ESTATE', 'RETAIL')) & (col('EXPOSURE_CURRENCY')!=col('BORROWER_INCOME_SOURCE_CURR')) & (col('RW').cast('double')*0.15 < 0.15), col('RW')*0.15).otherwise(\
                    col('RW')))        
            ).otherwise(col('RW')))

    return frame

def adjusted_coll_maturity(frame):
    frame = frame.withColumn('sumif1',\
        when((col('ELIGIBLE_CRM') == 'YES') & (col('COLL_ORIGINAL_MATURITY').cast('double') >=1) & (col('COLL_REMAINING_MATURITY').cast('double') >= 0.25) &\
            (col('COLL_REMAINING_MATURITY') < col('RESIDUAL_MATURITY'))    
            , sum(col('COLL_VALUE')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0)   
        )\
        .withColumn('mint2', least(col('RESIDUAL_MATURITY'), lit(5)).cast('double'))\
        .withColumn('if',\
            when((col('COLL_ORIGINAL_MATURITY').cast('double') >= 1) & (col('COLL_REMAINING_MATURITY').cast('double') >=0.25) & (col('ELIGIBLE_CRM') == 'YES') & (col('COLL_REMAINING_MATURITY') < col('RESIDUAL_MATURITY')),\
                col('COLL_REMAINING_MATURITY'))            
                .otherwise(0))\
        .withColumn('minif',\
            min(col('if')).over(Window.partitionBy('CUSTOMER_ID')))\
        .withColumn('finalif',\
            least((least(col('minif'), col('mint2'))/col('mint2') - 0.25), lit(0))
            )\
        .withColumn('sumif2',\
            when((col('ELIGIBLE_CRM') == 'YES') & (col('COLL_ORIGINAL_MATURITY').cast('double') >=1) & (col('COLL_REMAINING_MATURITY').cast('double') >= 0.25) &\
                (col('COLL_REMAINING_MATURITY') >= col('RESIDUAL_MATURITY'))    
                , sum(col('COLL_VALUE')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0)
            )\
        .withColumn('ADJUSTED_COLL_MATURITY', 
        col('sumif1')*col('EXPOSURE %')*col('finalif') + col('sumif2')).drop('sumif1', 'sumif2', 'minif', 'if', 'finalif')

    frame = frame.fillna({'ADJUSTED_COLL_MATURITY': 0})
    return frame

def final_adjusted_coll(frame, collateral):
    key_mapping = make_spark_mapping('9. REG TABLE', 'KEY CONFIGURATIONS')
    value = float(key_mapping.select('VALUE').collect()[0][0])

    key1, key2 = 'CUSTOMER_ID', 'CUSTOMER_ID'
    cols = ['ALLOCATED_COLLATERAL_AFTER_MT_MISMATCH_AND_HAIRCUT']
    frame = join_frame(frame, collateral, key1, key2, cols)
    # frame = frame.joim()

    frame = frame.fillna( {'ALLOCATED_COLLATERAL_AFTER_MT_MISMATCH_AND_HAIRCUT':0, 'COLL_CCY': 'N/A'})
    frame = frame.withColumn('tmpsum1',\
        when(col('COLL_CCY') == col('EXPOSURE_CURRENCY'), sum(col('ALLOCATED_COLLATERAL_AFTER_MT_MISMATCH_AND_HAIRCUT')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0)  
        )\
        .withColumn('tmpsum2',\
        when(col('COLL_CCY') != col('EXPOSURE_CURRENCY'), sum(col('ALLOCATED_COLLATERAL_AFTER_MT_MISMATCH_AND_HAIRCUT')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0))
    frame = frame.fillna({'tmpsum1': 0, 'tmpsum2': 0})
    frame = frame.withColumn('FINAL_ADJUSTED_COLL',\
        col('tmpsum1')* col('EXPOSURE %') + col('tmpsum2')*col('EXPOSURE %')*(1-value)).drop('tmpsum1', 'tmpsum2')
    # frame.select('tmpsum1', 'tmpsum2', 'FINAL_ADJUSTED_COLL').show()
    return frame

def netting_value_adjusted(frame):
    netting = read_excel(source.data_path['netting'])
    key1, key2 = 'EXPOSURE_ID', 'EXPOSURE_ID'
    cols = ['NETTING_EXPOSURE_VALUE']
    frame = join_frame(frame, netting, key1, key2, cols)
    frame = frame.withColumn('NETTING_VALUE_ADJUSTED',\
        sum(frame['NETTING_EXPOSURE_VALUE']).over(Window.partitionBy('EXPOSURE_ID')))\
        .withColumn('NETTING_VALUE_ADJUSTED',\
            when(col('NETTING_VALUE_ADJUSTED').isNull(), 'CHECK ERROR').otherwise(col('NETTING_VALUE_ADJUSTED'))).drop('NETTING_EXPOSURE_VALUE')
    return frame

def adjusted_guarantee_maturity(frame, guarantee):
    # guarantee = read_excel(source.data_path['guarantee'])
    cols = ['GUARANTEE_ORIGINAL_MATURITY',
    'GUARANTEE_REMAINING_MATURITY',
    'ELIGIBLE_GUARANTEE_FLAG',
    'ALLOCATED_GUARANTEE_AFTER_MT_MISMATCH',
    'CURRENCY_OF_GUARANTEE',
    'GUARANTOR_RW',
    'GUARANTEE_RWA']
    
    key1, key2 = 'CUSTOMER_ID', 'CUSTOMER_ID'

    frame = join_frame(frame, guarantee, key1, key2, cols)
    frame = frame.withColumn('sumif1',\
        when((col('ELIGIBLE_GUARANTEE_FLAG') == 'Y') & (col('GUARANTEE_ORIGINAL_MATURITY').cast('double') >=1) & (col('GUARANTEE_REMAINING_MATURITY').cast('double') >= 0.25) &\
            (col('GUARANTEE_REMAINING_MATURITY') < col('RESIDUAL_MATURITY'))    
            , sum(col('ASSET_CLASS_MANUAL')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0)   
        )\
        .withColumn('mint2', least(col('RESIDUAL_MATURITY'), lit(5)).cast('double'))\
        .withColumn('if',\
            when((col('GUARANTEE_ORIGINAL_MATURITY').cast('double') >= 1) & (col('GUARANTEE_REMAINING_MATURITY').cast('double') >=0.25) & (col('ELIGIBLE_GUARANTEE_FLAG') == 'Y') & (col('GUARANTOR_RW') < col('RW_CC')),\
                col('COLL_REMAINING_MATURITY'))            
                .otherwise(0))\
        .withColumn('minif',\
            min(col('if')).over(Window.partitionBy('CUSTOMER_ID')))\
        .withColumn('finalif',\
            least((least(col('minif'), col('mint2'))/col('mint2') - 0.25), lit(0))
            )\
        .withColumn('sumif2',\
            when((col('ELIGIBLE_CRM') == 'YES') & (col('COLL_ORIGINAL_MATURITY').cast('double') >=1) & (col('COLL_REMAINING_MATURITY').cast('double') >= 0.25) &\
                (col('COLL_REMAINING_MATURITY') >= col('RESIDUAL_MATURITY'))    
                , sum(col('GUARANTEE_REMAINING_MATURITY')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0)
            )\
        .withColumn('ADJUSTED_GUARANTEE_MATURITY', 
        col('sumif1')*col('EXPOSURE %')*col('finalif') + col('sumif2')).drop('sumif1', 'sumif2', 'mint2', 'minif', 'if', 'finalif')

    frame = frame.fillna({'ADJUSTED_GUARANTEE_MATURITY': 0})
    return frame

def final_adjusted_guarantee(frame):
    key_mapping = make_spark_mapping('9. REG TABLE', 'KEY CONFIGURATIONS')
    value = float(key_mapping.select('VALUE').collect()[0][0])

    frame = frame.withColumn('tmp',\
        when(col('GUARANTOR_RW') < col('RW_CC'), sum(col('ALLOCATED_GUARANTEE_AFTER_MT_MISMATCH')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0)    
        )\
        .withColumn('FINAL_ADJUSTED_GUARANTEE',\
        when(col('CURRENCY_OF_GUARANTEE') == col('EXPOSURE_CURRENCY'), col('tmp')*col('EXPOSURE %')).otherwise(\
            col('tmp')*col('EXPOSURE %')*(1- value))       
            ).drop('tmp')

    return frame

def ead_before_crm_on_bs(frame):
    colsum = ['OUTSTANDING_AMT_LCY / UTILIZED LIMIT', 'ACCRUED_INTEREST_LCY', 'OUTSTANDING_FEES_PENALITIES_LCY', 'SPECIFIC_PROVISION']
    frame = frame.withColumn('EAD BEFORE CRM (ON-BS)',\
        when(is_error(col('OUTSTANDING_AMT_LCY / UTILIZED LIMIT')) == 1, 0).otherwise(\
            when(is_error(col('CCF')) == 1,\
                when((is_error(col('SPECIFIC_PROVISION'))==1), reduce(add, [col(x) for x in colsum[:-1]])).otherwise(\
                    greatest(lit(0), reduce(add, [col(x) for x in colsum])))           
                ).otherwise(0)
            )    
        )
    return frame

def ead_before_crm_off_bs(frame):
    ccf_mapping = make_spark_mapping('9. REG TABLE', 'CCF TABLE')
    ccf_mapping = ccf_mapping.where(col('CCF_CD')!='NaN')
    ccf_dct = make_lookup(ccf_mapping, 'CCF_CD', 'CCF%')

    colsum  = ['OUTSTANDING_AMT_LCY / UTILIZED LIMIT', 'ACCRUED_INTEREST_LCY', 'OUTSTANDING_FEES_PENALITIES_LCY',\
        'SPECIFIC_PROVISION']
    frame = frame.withColumn('t',\
        map_dct(ccf_dct)(col('CCF')))\
        .withColumn('EAD BEFORE CRM (OFF-BS)',\
            when((col('OUTSTANDING_AMT_LCY / UTILIZED LIMIT')=='NA') | (is_error('CCF')==1), 0).otherwise(\
                when(is_error(col('SPECIFIC_PROVISION'))==1, map_dct(ccf_dct)(col('SPECIFIC_PROVISION'))*reduce(add, [col(x) for x in colsum[:-1]])).otherwise(\
                    greatest(lit(0), map_dct(ccf_dct)(col('SPECIFIC_PROVISION'))*reduce(add, [col(x) for x in colsum])))
            )).drop('t')

    return frame

def ead_after_crm_on_bs(frame):
    frame = frame.withColumn('tt',\
        when(is_error(col('CCF')) ==1, col('EAD BEFORE CRM (ON-BS)') - col('FINAL_ADJUSTED_GUARANTEE') - col('FINAL_ADJUSTED_COLL') - col('NETTING_VALUE_ADJUSTED')).otherwise(0))\
        .withColumn('EAD AFTER CRM (ON-BS)',\
            greatest(lit(0), col('tt'))).drop('tt')
    return frame

def ead_after_crm_off_bs(frame):
    frame = frame.withColumn('tt',\
        when(is_error(col('CCF')) ==0, col('EAD BEFORE CRM (OFF-BS)') - col('FINAL_ADJUSTED_GUARANTEE') - col('FINAL_ADJUSTED_COLL') - col('NETTING_VALUE_ADJUSTED')).otherwise(0))\
        .withColumn('EAD AFTER CRM (OFF-BS)',\
            greatest(lit(0), col('tt'))).drop('tt')

    return frame

def rwa_on_bs(frame):
    frame = frame.withColumn('tt', \
        when(col('GUARANTOR_RW') < col('RW_CC'), sum(col('GUARANTEE_RWA')).over(Window.partitionBy('CUSTOMER_ID'))).otherwise(0))\
        .withColumn('ttt',\
        when(is_error(col('CCF')) == 1, col('EAD AFTER CRM (ON-BS)')*col('RW_CC')+col('tt')*col('EXPOSURE %')).otherwise(0))\
        .withColumn('RWA_ON_BS', greatest(lit(0), col('ttt'))).drop('ttt')
    return frame

def rwa_off_bs(frame):
    frame = frame.withColumn('ttt',\
        when(is_error(col('CCF')) == 0, col('EAD AFTER CRM (ON-BS)')*col('RW_CC')+col('tt')*col('EXPOSURE %')).otherwise(0))\
        .withColumn('RWA_OFF_BS', greatest(lit(0), col('ttt'))).drop('tt', 'ttt', 'GUARANTOR_RW', 'GUARANTEE_RWA')
    return frame

def final_cols(frame):
    frame = frame.withColumn('TOTAL_RWA', col('RWA_ON_BS') + col('RWA_OFF_BS'))\
        .withColumn('RWA_ON_BS Without CRM', col('EAD BEFORE CRM (ON-BS)')* col('RW_CC'))\
        .withColumn('RWA_OFF_BS Without CRM', col('EAD BEFORE CRM (OFF-BS)')*col('RW_CC'))\
            .withColumn('TOTAL_RWA Without CRM', col('RWA_OFF_BS Without CRM') + col('RWA_ON_BS Without CRM'))
    return frame



############################33
def transactor_flag_od():
    frame = read_excel(source.data_path['od'])

    cols1 = frame.columns[4:15]
    cols2 = frame.columns[3:14]

    frame = frame.withColumn("numeric_count", reduce(add, [check_numeric(col(x)) for x in frame.columns[3:-1]]))\
    .withColumn('check_zero', reduce(add, [check_zero(col(x)) for x in frame.columns[3:-1]]))\
    .withColumn('check_if', reduce(add, [check_divide(col(x), col(y)) for x, y in zip(cols1, cols2)]))\
    .withColumn('Transactor flag',\
        when(col('check_zero')>1, 'N').otherwise(when((col('numeric_count') == 12) & (col('check_if')<1), 'Y').otherwise('N'))
        ).drop('numeric_count', 'check_zero', 'check_if')

    return frame

def transactor_flag_cc():
    frame = read_excel(source.data_path['cc'])

    cols1 = frame.columns[4:16]
    cols2 = frame.columns[17:29]

    cols4 = frame.columns[18:30]
    cols3 = frame.columns[5:17]

    frame = frame.withColumn('count1',\
        reduce(add, [check_numeric(col(x)) for x in cols1]) + reduce(add, [check_numeric(col(x)) for x in cols2]))\
        .withColumn('minus1', reduce(add, [check_zero(col(y) - col(x)) for x, y in zip(cols1, cols2)]))\
        .withColumn('count2',\
        reduce(add, [check_numeric(col(x)) for x in cols3]) + reduce(add, [check_numeric(col(x)) for x in cols4]))\
        .withColumn('minus2', reduce(add, [check_zero(col(y) - col(x)) for x, y in zip(cols3, cols4)]))\
        .withColumn('Transactor flag',\
        when(col('Tag_trano').isin('M', 'JCB'),\
            when((col('count1') == 24) & (col('minus1') == 0), 'Y').otherwise('N')
            ).otherwise(\
                when((col('count2') == 24) & (col('minus2') == 0), 'Y').otherwise('N')
                )).drop('cunt1', 'count2', 'minus1', 'minus2')

    return frame
