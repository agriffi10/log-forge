from datetime import datetime

class Log():

    def __init__(self):
        self.log_created = datetime.now()
        self.message = ""
        self.type = ""
        self.log_closed = None

    def close_log(self):
        self.log_closed = datetime.now()

    def print_log(self):
        log_stamp = datetime.now()
        print(self.message)