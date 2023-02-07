import logging
import os 
if not os.path.isdir('logs'): os.mkdir('logs')

log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
logFile = './logs/logs.log'

file_handler = logging.FileHandler(logFile)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)

log = logging.getLogger('root')
log.setLevel(logging.INFO)

log.addHandler(file_handler)
log.addHandler(stream_handler)