import json
import logging
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, List
import pathlib
import datetime


class JSONFormatter(logging.Formatter):
    '''
    Helper formatter that outputs JSON strings with consistent fields
    '''
    def format(self, record):
        log_obj = {
            "timestamp": datetime.datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "function": record.funcName,
            "line_no": record.lineno
        }
        ## add exception ##
        if record.exc_info:
            log_obj['exception'] = self.formatException(record.exc_info)
        ## add extra fields if passed ##
        if hasattr(record, 'error'):
            log_obj['error'] = record.error
        ## return JSON string ##
        return json.dumps(log_obj)

class PandabaseLogger:
    '''
    Centralized logging for the Pandabase package. Leverages the singleton
    pattern to ensure that all loggers are the same instance

    To use, call the logger object in the application, and configure
    once for the logging handle. For instance, to log to better stack:

    ## my app ##
    import pandabase
    from pandabase.logging.logger import logger
    
    logger.configure(handlers=[BetterStackHandler], level=logging.DEBUG)
    
    pandabase.do_something() <- this will log to better stack
    '''
    ## state for the singleton pattern ##
    _instance = None 
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    ## init ##
    def __init__(self):
        ## only init once ##
        if PandabaseLogger._initialized:
            return
        ## core class properties ##
        self.loggers = {}
        self.handlers: List[logging.Handler] = []
        self.level = logging.INFO
        ## default set up ##
        default_loc = '{0}/Logs'.format(pathlib.Path(__file__).parent.resolve())
        self.configure(log_dir=default_loc)
        ## set initialization to true to prevent re-init ##
        PandabaseLogger._initialized = True
    
    def configure(self,
        handlers:Optional[List[logging.Handler]] = None,
        level:int = logging.INFO,
        log_dir:Optional[str] = None
    ):
        '''
        Configures the logger with the given handlers and level
        
        Parameters:
        * handlers : list of logging.Handler objects
        * level : logging level
        * log_dir: optional override path for local logs
        '''
        self.handlers = handlers if handlers else []
        self.level = level
        formatter = JSONFormatter()
        ## default to console if no handlers are provided ##
        if not self.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(logging.WARNING)
            self.handlers = [console_handler]
        
        ## always append a handler for local logging ##
        log_dir = log_dir if log_dir else '{0}/Logs'.format(pathlib.Path(__file__).parent.resolve())
        log_file = '{0}/pandabase.log'.format(log_dir)
        file_handler = TimedRotatingFileHandler(
            filename=log_file,
            when='midnight',
            backupCount=7
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        self.handlers.append(file_handler)
    
    def get_logger(self, name: str) -> logging.Logger:
        '''
        Get a logger with the package's configuration

        Parameters:
        * name: name of the logger

        Returns:
        * logger: logger object
        '''
        if name not in self.loggers:
            logger = logging.getLogger('pandabase.{0}'.format(name))
            logger.setLevel(self.level)
            logger.handlers.clear()
            for handler in self.handlers:
                logger.addHandler(handler)
            self.loggers[name] = logger
        return self.loggers[name]

