import csv

# DataCat is a log extractor class 
class DataCat:
    def __init__(self, config):
        self.config = config
        self.datacat_header = ["timestamp", "type", "content", "extra_1", "extra_2"]

        
    def write_log(self, log_cycle, log_flow):
        if log_flow:
            # get the correct path for the log file
            log_cycle = self.config["data_path"] + f"/{log_cycle}"+ self.config["log_filename"]
            # add the header to the log flow
            log_flow.insert(0, ";".join(self.datacat_header) + "\n")

            with open(log_cycle, "a") as f:
                # acquire lock and write to the file, then release the lock
                # if self.acquire_lock(log_cycle):
                f.writelines(log_flow)
                # self.release_lock(log_cycle)

