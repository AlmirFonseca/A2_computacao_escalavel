
# User Behavior Analytics
import csv
class CadeAnalytics:
    def __init__(self, config):
        self.config = config
        self.cade_analytics_header = ["timestamp", "type", "content", "extra_1", "extra_2"]


    
    def write_log(self, num_cycle, log_flow):
        if log_flow:
            # get the correct path for the log file
            log_cycle = self.config["data_path"] + f"/{num_cycle}"+ self.config["request_filename"]
            # add the header to the log flow
            log_flow.insert(0, ";".join(self.cade_analytics_header) + "\n")
            
            with open(log_cycle, "a") as f:
                
                # acquire lock and write to the file, then release the lock
                # if self.acquire_lock(log_cycle):
                # write header
                f.writelines(log_flow)
                # self.release_lock(log_cycle)
