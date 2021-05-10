import logging
from logging.config import fileConfig

#This config is for production, for time being running this forecasting from sql server where creating some formatter error
#hence commenting this for time being and using the simple one.
'''fileConfig('logging_config.ini', disable_existing_loggers=False)
#Creating an object 
Logger=logging.getLogger('sLogger')'''

#This is only for debugging purpose
logging.basicConfig(filename='C:\\Temp\\FileSplitter\\FileSplitter.log',level=logging.DEBUG)
Logger=logging.getLogger()