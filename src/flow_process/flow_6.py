from src.columns_process.exposure_case_1 import *
from src.utils import *

def flow_exposure(path_exposure: str):
    exposure = read_excel(path_exposure)

    exposure = original_maturity(exposure)
    exposure = residual_maturity(exposure)
    exposure = cpty_type_cpty_sub_type_borrower_income_source_curr(exposure)
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
    exposure = final_adjusted_guarantee(exposure)
    exposure = ead_before_crm_on_bs(exposure)
    exposure = ead_before_crm_off_bs(exposure)
    exposure = ead_after_crm_on_bs(exposure)
    exposure = ead_after_crm_off_bs(exposure)
    exposure = rwa_on_bs(exposure)
    exposure = rwa_off_bs(exposure)
    exposure = final_cols(exposure)
    
    return exposure

# t1 = time.time()

# exposure = flow_exposure('/home/dieule/Desktop/input_test/EXPOSURE.xlsx')
# write_excel(exposure, '/output/exposure.xlsx')

# print(f'Process in {time.time() - t1}.2f')