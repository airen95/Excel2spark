from omegaconf import OmegaConf
from src.flow_process.flow_1 import flow_1 
from src.flow_process.flow_2 import flow_2 
import time
from src.utils import write_excel

cfg = OmegaConf.load('config/source.yaml')
# path_exposure = cfg.path_save
# path_save = cfg.path_save
select_columns = cfg.select_columns
paths = cfg.data_path

if __name__ == "__main__":
    
    t1 = time.time()

    #flow_1:
    table_1, table_2, table_3, table_od, table_cc, table_5, table_6 = flow_1(paths[0], paths[1], paths[2], paths[4], paths[5], paths[6], paths[7])    
    
    #flow_2:
    table_3, table_5, table_6, on_table, off_table = flow_2(table_3, table_5, table_6)

    print(f'Process in {time.time() - t1:.2f}')