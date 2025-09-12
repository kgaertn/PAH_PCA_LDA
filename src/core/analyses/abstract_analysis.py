class AbstractAnalyser:
    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        
    def run(self):
        raise NotImplementedError("Please Implement this method")
    
    def prepare(self):
        raise NotImplementedError("Please Implement this method")
    
    def handle_results():
        raise NotImplementedError("Please Implement this method")
    
    def reconstruct_results():
        raise NotImplementedError("Please Implement this method")        
    
    def _upload_step(self, entry, analysis_name, upload_func, result):
        if not self.logger.is_uploaded(entry, analysis_name):
            upload_func(result)
            self.logger.mark_uploaded(entry, analysis_name)