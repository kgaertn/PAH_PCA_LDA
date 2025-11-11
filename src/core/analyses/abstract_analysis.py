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

    def _upload_step(self, exp_params, analysis_params, uploads):
        """
        uploads: list of (upload_func, result) tuples
        """
        #logged = entry["uploaded_steps"].get(analysis_name)
        #params = logged.get("params")
        #dependencies = 
        
        #if not self.logger.is_uploaded(entry, analysis_name, params):
        for upload_func, result in uploads:
            upload_func(result)
            #self.logger.mark_uploaded(entry, analysis_name, params)
