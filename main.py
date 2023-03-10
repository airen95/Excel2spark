from omegaconf import OmegaConf
from src.flow_process.flow_1 import flow_1 
from src.flow_process.flow_2 import flow_2 
import time
from src.utils import write_excel

cfg = OmegaConf.load('config/source.yaml')
path_exposure = cfg.path_save
path_save = cfg.path_save
select_columns = cfg.select_columns

if __name__ == "__main__":
    
    t1 = time.time()
    # exposure = flow(path_exposure)
    # exposure = exposure.select(*select_columns)
    # write_excel(exposure, path_save)
    on_table, off_table = flow_credit_output(path_exposure)
    on_table.to_csv('on_table.csv')
    off_table.to_csv('off_table.csv')
    print(f'Process in {time.time() - t1:.2f}')