from core.musashi import *

log_analyzer = LogAnalyzer(
    log_path='evtx-attack-samples/Discovery', 
    result_path='output', 
    log_format='winevtx', 
    sigma_rule_path=None, # if not specified will use default path
    num_threads=None
)

log_analyzer.execute()
