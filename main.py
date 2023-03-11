from omegaconf import OmegaConf
from src.flow_process.flow_1 import flow_1 
from src.flow_process.flow_2 import flow_2 
import time
from src.utils import write_excel

cfg = OmegaConf.load('config/source.yaml')
select_columns = cfg.select_columns
path_1 = cfg.data_path['customer']
path_2 = cfg.data_path['scra']
path_3 = cfg.data_path['collateral']
path_od = cfg.data_path['od']
path_cc = cfg.data_path['cc']
path_5 = cfg.data_path['guarantee']
path_6 = cfg.data_path['exposure']


if __name__ == "__main__":
    
    t1 = time.time()

    #flow_1:
    table_1, table_2, table_3, table_od, table_cc, table_5, table_6 = flow_1(path_1, path_2, path_3, path_5, \
                                                                            path_od, path_cc, path_6)    
    
    write_excel(table_1, path_1)
    write_excel(table_2, path_2)
    write_excel(table_od, path_od)
    write_excel(table_cc, path_cc)

    #flow_2:
    table_3, table_5, table_6, on_table, off_table = flow_2(table_3, table_5, table_6)
    
    write_excel(table_3, path_3)
    write_excel(table_5, path_5)
    write_excel(table_6, path_6)

    print(f'Process in {time.time() - t1:.2f}')