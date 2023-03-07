from omegaconf import OmegaConf
from src.flow_process import flow
import time
from src.utils import write_excel

cfg = OmegaConf.load('config/source.yaml')
path_exposure = cfg.data_path['exposure']
path_save = cfg.path_save
select_columns = cfg.select_columns

if __name__ == "__main__":
    
    t1 = time.time()
    exposure = flow(path_exposure)
    exposure = exposure.select(*select_columns)
    write_excel(exposure, path_save)
    print(f'Process in {time.time() - t1:.2f}')