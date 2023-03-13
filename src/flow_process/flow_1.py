from src.columns_process.columns_scra import *
from src.columns_process.columns_customer import *
from src.columns_process.columns_guarantee import *
from src.columns_process.columns_collateral import *
from src.columns_process.credit_output import *
from src.columns_process.exposure_od_cc import *
from src.utils import *
from src.flow_process.flow_2 import *



#flow 1
# 2, 1, 4, OD, CC, 3(bỏ cột cuối) ,5 ( bỏ 2 cuối) , 6 (AT)

def flow_table_1(path: str, table_2):
    """Table 1"""
    frame = read_excel(path)
    # frame = frame.where(~col('CUSTOMER_ID').isNull())

    # frame = frame.where(~col('CUSTOMER_ID').isNull())
    frame = cpty_type_cpty_sub_type(frame)
    frame = cust_rating_cd(frame, table_2)
    # write_excel(frame, path)   
    return frame

def flow_table_2():
    """Table 2"""
    frame = make_spark_mapping_SCRA('2. SCRA', 'SCRA')
    frame = scra_group(frame)
    frame = frame.where(~col('CUSTOMER_ID').isNull())
    # write_excel(frame, path)     
    return frame

def flow_od():
    """Table OD"""
    # frame = read_excel(path_od)
    frame = transactor_flag_od()
    # write_excel(frame, path_od)
    return frame

def flow_cc():
    """Table CC"""
    # frame = read_excel(path_cc)
    frame = transactor_flag_cc()
    # write_excel(frame, path_cc)
    return frame


def flow_table_3_for_flow1(path: str):
    """Table 3 without last col"""
    frame = read_excel(path)

    frame = ori_mature_remaining_mature(frame)
    frame = coll_remaining_origin_maturity(frame)
    frame = rating_cd(frame)
    frame = crm_cd_eligible_crm(frame)
    frame = haircut_cd(frame)
    frame = haircut_percent(frame)
    frame = crm_eligible(frame)
    # frame = final_col(frame)
    # write_excel(frame, path)
    return frame


def flow_table_5_for_flow1(path: str):
    """Table 5 without last 2 cols"""
    frame = read_excel(path)
    frame = ori_mature_remain_mature_crm_eligible(frame)
    # frame = allocate_guarantee(frame)
    # frame = guarantee_rwa(frame)
    # write_excel(frame, path)
   
    return frame

def flow_table_6_for_flow1(path_exposure: str, table_1, table_3, table_5, table_od, table_cc):
    """Table 6 to column AQ"""
    exposure = read_excel(path_exposure)
    exposure = exposure.where(~col('CUSTOMER_ID').isNull())

    exposure = original_maturity(exposure)
    exposure = residual_maturity(exposure)
    exposure = cpty_type_cpty_sub_type_borrower_income_source_curr(exposure, table_1)
    exposure = loan_retail_exposure_secured_by_real_estate(exposure)
    exposure = reg_retail_8b_flag(exposure)
    exposure = transactor_flag(exposure, table_od, table_cc)
    exposure = specific_provision(exposure)
    exposure = regulatory_retail_flag_b4(exposure)
    exposure = prd_cd_prd_sub_cd(exposure)
    exposure = asset_class_asset_sub_class(exposure)
    exposure = specific_provision_bucket(exposure)
    exposure = cust_rating(exposure, table_1)
    exposure = exposure_percent(exposure)
    exposure = ltv(exposure, table_3)
    exposure = ltv_bucket(exposure)
    exposure = ccf(exposure)
    exposure = re_eligible_p60(exposure)
    exposure = key_category(exposure)
    exposure = rw(exposure)
    exposure = rw_cc(exposure)
    exposure = adjusted_coll_maturity(exposure)
    # exposure = final_adjusted_coll(exposure, table_3)
    # exposure = netting_value_adjusted(exposure)
    # exposure = adjusted_guarantee_maturity(exposure, table_5)
   
    # exposure = final_adjusted_guarantee(exposure)
    # exposure = ead_before_crm_on_bs(exposure)
    # exposure = ead_before_crm_off_bs(exposure)
    # exposure = ead_after_crm_on_bs(exposure)
    # exposure = ead_after_crm_off_bs(exposure)
    # exposure = rwa_on_bs(exposure)
    # exposure = rwa_off_bs(exposure)
    # exposure = final_cols(exposure)
    
    return exposure

def flow_1(path_1, path_3, path_5, path_exposure):
    table_2 = flow_table_2()
    table_1 = flow_table_1(path_1, table_2)
    table_od = flow_od()
    table_cc = flow_cc()
    table_3 = flow_table_3_for_flow1(path_3)
    table_5 = flow_table_5_for_flow1(path_5)
    table_6 = flow_table_6_for_flow1(path_exposure, table_1, table_3, table_5, table_od, table_cc)
    print('---------Done flow 1 --------------')
    return table_1, table_2, table_3, table_od, table_cc, table_5, table_6