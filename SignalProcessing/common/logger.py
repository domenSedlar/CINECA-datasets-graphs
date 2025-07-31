import logging
import os

class Logger:
    def __init__(self, name=__name__, log_file='pipeline.log', log_dir='logs'):
        self.logger_fake = logging.getLogger("idk")

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')

            # Console handler
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter('[%(filename)s] %(levelname)s: %(message)s'))
            ch.setLevel(logging.INFO)
            self.logger.addHandler(ch)

            # File handler
            import datetime
            self.log_dir = log_dir

            current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            log_filename = f"{os.path.splitext(os.path.basename(name if name != '__main__' else 'main'))[0]}.log"
            log_path = os.path.join(os.getcwd(), self.log_dir, current_time)
            os.makedirs(log_path, exist_ok=True)
            log_file = os.path.join(log_path, log_filename)
            fh = logging.FileHandler(log_file)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message) 

    def get_logger(self):
        """Expose the internal `logging.Logger` instance."""
        return self.logger_fake
    
    def get_logger_real(self):
        return self.logger