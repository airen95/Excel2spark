from src.columns_process.columns_scra import *
from src.columns_process.columns_customer import *
from src.columns_process.columns_guarantee import *
from src.columns_process.columns_collateral import *
from src.columns_process.credit_output import *
from src.columns_process.exposure_od_cc import *
from src.utils import *



#flow 1
# 1, 2, 4, OD, CC, 3(bỏ cột cuối) ,5 ( bỏ 2 cuối) , 6 (AT)

def flow_table_1(path: str):
    """Table 1"""
    frame = read_excel(path)
    frame = cpty_type_cpty_sub_type(frame)
    
    return frame

def flow_table_2():
    """Table 2"""
    frame = make_spark_mapping_SCRA('2. SCRA', 'SCRA')
    frame = scra_group(frame)
    
    return frame

def flow_od(path_od: str):
    """Table OD"""
    frame = read_excel(path_od)
    frame = transactor_flag_od(frame)
    return frame

def flow_cc(path_cc: str):
    """Table CC"""
    frame = read_excel(path_cc)
    frame = transactor_flag_cc(frame)
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

    return frame


def flow_table_5_for_flow1(path: str):
    """Table 5 without last 2 cols"""
    frame = read_excel(path)
    frame = ori_mature_remain_mature_crm_eligible(frame)
    # frame = allocate_guarantee(frame)
    # frame = guarantee_rwa(frame)
    
    return frame

def flow_table_6_for_flow1(path_exposure: str):
    """Table 6 to column AT"""
    exposure = read_excel(path_exposure)

    exposure = original_maturity(exposure)
    exposure = residual_maturity(exposure)
    exposure = cpty_type_cpty_sub_type_borrower_income_source_curr(exposure)
    #----------------------------------------------
    exposure = exposure.withColumn('ASSET_CLASS_MANUAL', lit('DMDM')) #temporary
    # #-----------------------------------------------
    exposure = loan_retail_exposure_secured_by_real_estate(exposure)
    exposure = reg_retail_8b_flag(exposure)
    exposure = transactor_flag(exposure)
    exposure = specific_provision(exposure)
    exposure = regulatory_retail_flag_b4(exposure)
    exposure = prd_cd_prd_sub_cd(exposure)
    exposure = asset_class_asset_sub_class(exposure)
    exposure = specific_provision_bucket(exposure)
    exposure = cust_rating(exposure)
    exposure = exposure_percent(exposure)
    exposure = ltv(exposure)
    exposure = ltv_bucket(exposure)
    exposure = ccf(exposure)
    exposure = re_eligible_p60(exposure)
    exposure = key_category(exposure)
    exposure = rw(exposure)
    exposure = rw_cc(exposure)
    exposure = adjusted_coll_maturity(exposure)
    exposure = final_adjusted_coll(exposure)
    exposure = netting_value_adjusted(exposure)
    exposure = adjusted_guarantee_maturity(exposure)
    
    # exposure = final_adjusted_guarantee(exposure)
    # exposure = ead_before_crm_on_bs(exposure)
    # exposure = ead_before_crm_off_bs(exposure)
    # exposure = ead_after_crm_on_bs(exposure)
    # exposure = ead_after_crm_off_bs(exposure)
    # exposure = rwa_on_bs(exposure)
    # exposure = rwa_off_bs(exposure)
    # exposure = final_cols(exposure)
    
    return exposure

def flow_1():
    pass