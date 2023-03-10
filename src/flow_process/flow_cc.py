from columns_process.exposure_od_cc import transactor_flag_od
from .utils import *

def flow_cc(path_cc: str):
    frame = read_excel(path_cc)
    frame = transactor_flag_cc(frame)
    return frame