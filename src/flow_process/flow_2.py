from src.columns_process.columns_scra import *
from src.columns_process.columns_customer import *
from src.columns_process.columns_guarantee import *
from src.columns_process.columns_collateral import *
from src.columns_process.credit_output import *
from src.columns_process.exposure_od_cc import *
from src.utils import *
from src.constmap import keywords, horizontal_cols
import pandas as pd


# flow 2:
# 3 cột cuối, 5 cột O, bảng 6 (AU), bảng 5 cột P, bảng 6, bảng 11

def flow_table_3_for_flow2(frame):
    """Table 3 with last col"""
    frame = final_col(frame)

    return frame


def flow_table_5_for_col_O(frame):
    """Table 5 with last 2 cols"""
    frame = allocate_guarantee(frame)
    # frame = guarantee_rwa(frame)
    
    return frame, 

def flow_table_6_for_flow2(frame):
    """Table 6 to column AU"""
    
    exposure = final_adjusted_guarantee(frame)
    exposure = ead_before_crm_on_bs(exposure)
    exposure = ead_before_crm_off_bs(exposure)
    exposure = ead_after_crm_on_bs(exposure)
    exposure = ead_after_crm_off_bs(exposure)
    exposure = rwa_on_bs(exposure)
    exposure = rwa_off_bs(exposure)
    exposure = final_cols(exposure)
    
    return exposure

def flow_table_5_for_col_P(frame):
    """Table 5 with last 2 cols"""
    frame = guarantee_rwa(frame)
    
    return frame,

def flow_table_11(path_exposure: str):
    exposure = read_excel(path_exposure)
    on, off = table_output(exposure)

    on_table = pd.DataFrame()
    off_table = pd.DataFrame()

    on_table['ASSESS_CLASS'] = horizontal_cols
    off_table['ASSESS_CLASS'] = horizontal_cols

    for col_1, data_1 in on.items():
        on_table[col_1] = data_1

    for col_1, data_1 in off.items():
        off_table[col_1] = data_1

    on_table['Total'] = on_table.apply(lambda x: x['OUTSTANDING AMOUNT'] + x['ACCURED INTEREST']+x['OUTSTANDING FEE'], axis = 1)
    off_table['Total'] = off_table.apply(lambda x: x['OUTSTANDING AMOUNT'] + x['ACCURED INTEREST']+x['OUTSTANDING FEE'], axis = 1)
       
    return on_table, off_table

def flow_2():
    pass