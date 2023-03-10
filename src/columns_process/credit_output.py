from src.constmap import cols, output_config, vertical_cols, keywords
from pyspark.sql.functions import *
from operator import add
from functools import reduce

def compare_n_sum(frame_in, col2sum: str, value: str):
    t = frame_in.filter(col('ASSET_CLASS_MANUAL') == value).agg(sum(col(col2sum)).alias('result'))
    return t.select('result').collect()[0][0]

# def amt(aed_on: list, aed_off: list) -> list:
#     amt_on, amt_off = [], []
#     for i, j in zip(aed_on, aed_off):
#         if i == 0 and j == 0:
#             amt_on.append(i)
#             amt_off.append(j)
#         else:
#             amt_on.append(i/(i+j) + i)
#             amt_off.append(j/(i+j) + j)
#     return amt_on, amt_off

def make_first_dct(frame_in, name: str,  mode: str):
    col2sum = output_config[mode][name]
    tmp = {}
    for kw in keywords:
        res = compare_n_sum(frame_in, col2sum, kw)
        if not res:
            res = 0
        tmp[kw] = res
    return tmp

def dct2cols(dct):
    tmp = []
    for _, values in cols.items():
        tmp.append(reduce(add, [dct[i] for i in values]))
    return tmp

def make_column(frame_in, column_name: str, mode: str, col_de_on: list = None, col_de_off: list = None):
    dct = make_first_dct(frame_in,column_name, mode)
    f_column = dct2cols(dct)
    if not col_de_on:
        return f_column
    if mode == 'on':
        f_column = [0 if (i ==0 and j == 0 and k ==0) else (i/ (i+j)*k) for i, j, k in zip(col_de_on, col_de_off, f_column)]
    else:
        f_column = [0 if (i ==0 and j == 0 and k ==0) else (j/ (i+j)*k) for i, j, k in zip(col_de_on, col_de_off, f_column)]
    return f_column

def table_output(frame_in):
    # read_exposure:
    # exposure = read_excel('/home/dieule/Desktop/code/excel/output/EXPOSURE.xlsx')
    results_on, results_off = {}, {}
    for coll in vertical_cols:
        if coll in ['EAD BEFORE CRM','RWA WITH CRM', 'RWA WITH CRM', 'RWA WITHOUT CRM', \
                    'OUTSTANDING AMOUNT', 'ACCURED INTEREST', 'OUTSTANDING FEE', 'EAD AFTER CRM']:
            try:
                off = make_column(frame_in, coll, 'off')
                on = make_column(frame_in, coll, 'on')
            except:
                off = [0]*26
                on = make_column(frame_in, coll, 'on')
        else:
            on = make_column(frame_in, coll, 'on', results_on['EAD BEFORE CRM'], results_off['EAD BEFORE CRM'])
            off = make_column(frame_in, coll, 'off', results_on['EAD BEFORE CRM'], results_off['EAD BEFORE CRM'])
            
        results_on[coll] = on
        results_off[coll] = off

        # print(results_on)
        # print(results_off)
    return results_on, results_off