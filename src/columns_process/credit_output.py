from src.constmap import assess_class, output_config, vertical_cols, keywords, total_cols2sum, horizontal_cols
from src.utils import read_excel
import pandas as pd


def groupby_frame(spark_frame):
    cols = {i: 'sum' for i in total_cols2sum}
    pd_frame = spark_frame.groupby('ASSET_SUB_CLASS').agg(cols).toPandas()
    pd_frame  = pd_frame.fillna(0)
    return pd_frame

def kw_dct(pd_frame):
    tmp = {}
    for col in total_cols2sum:
        name = 'sum('+col+')'
        tmp[col] = {}
        for kw in keywords:
            try:
                tmp[col][kw] = pd_frame[pd_frame['ASSET_SUB_CLASS'] == kw][name].values[0]
            except:
                tmp[col][kw] = 0
    return tmp

# def sumpython(lst):
#     s = 0
#     for i in lst:
#         s+=i
#     return s

def total(lst_to_sum, lst_index):
    return sum([lst_to_sum[i] for i in lst_index])

def dct2cols(dct):
    tmp = []
    for _, values in assess_class.items():
        tmp.append(sum([dct[i] for i in values]))
    return tmp

def make_column(column_name: str, mode: str, dct: dict, col_de_on: list = None, col_de_off: list = None):
    f_column = dct2cols(dct)
    if not col_de_on:
        return f_column
    if mode == 'on':
        f_column = [0 if (i ==0 and j == 0 and k ==0) else (i/ (i+j)*k) for i, j, k in zip(col_de_on, col_de_off, f_column)]
    else:
        f_column = [0 if (i ==0 and j == 0 and k ==0) else (j/ (i+j)*k) for i, j, k in zip(col_de_on, col_de_off, f_column)]
    return f_column


def table_output(spark_frame):
    pd_frame = groupby_frame(spark_frame)
    total_dct = kw_dct(pd_frame)    
    # read_exposure:
    # exposure = read_excel('/home/dieule/Desktop/code/excel/output/EXPOSURE.xlsx')
    results_on, results_off = {}, {}
    for coll in vertical_cols:
        if coll in ['EAD BEFORE CRM','RWA WITH CRM', 'RWA WITH CRM', 'RWA WITHOUT CRM', \
                    'OUTSTANDING AMOUNT', 'ACCURED INTEREST', 'OUTSTANDING FEE', 'EAD AFTER CRM']:
            try:
                col_name_on = output_config['on'][coll]
                col_name_off = output_config['off'][coll]
                if col_name_on == col_name_off:
                    dct = total_dct[col_name_on]
                    on = make_column(coll, 'on', dct)
                    off = on
                else:
                    on = make_column(coll, 'on', total_dct[col_name_on])
                    off = make_column(coll, 'off', total_dct[col_name_off])
            except:
                off = [0]*26
                col_name_on = output_config['on'][coll]
                on = make_column(coll, 'on', total_dct[col_name_on])
        else:
            col_name_on = output_config['on'][coll]
            col_name_off = output_config['off'][coll]
            on = make_column(coll, 'on', total_dct[col_name_on], results_on['EAD BEFORE CRM'], results_off['EAD BEFORE CRM'])
            off = make_column(coll, 'off',total_dct[col_name_off],results_on['EAD BEFORE CRM'], results_off['EAD BEFORE CRM'])

        lst_index = [0,1, 4, 5, 6, 7, 10, 15, 23, 24, 25]
        on.append(total(on, lst_index ))
        off.append(total(off, lst_index ))
          
        results_on[coll] = on
        results_off[coll] = off

        # print(results_on)
        # print(results_off)
    return results_on, results_off

def final_table(dct: dict):
    table = pd.DataFrame([])
    table['ASSESS CLASS'] = horizontal_cols
    for col_name, value in dct.items():
        table[col_name] = value
    return table

def credit_output(path_out_6, path_on, path_off):
    spark_frame = read_excel(path_out_6)
    results_on, results_off = table_output(spark_frame)

    table_on = final_table(results_on)
    table_off = final_table(results_off)
    table_on.to_csv(path_on)
    table_off.to_csv(path_off)

    # return table_on, table_off
