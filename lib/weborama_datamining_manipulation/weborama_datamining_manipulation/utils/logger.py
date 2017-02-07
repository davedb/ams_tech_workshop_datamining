import logging
from os import makedirs, path

class Logger:
    """Logging Class

    :param config: dict ``{'IS_DEV': bool, 'IS_PREPROD': bool, 'IS_PROD': bool, 'LOGGING_FILE': str}``

    Usage::

        >>> import logger.Logger
        >>> loggerFactory = logger.Logger(logger_config)
        >>> myLogger = loggerFactory.create_logger('Pippo')
    """
    def __init__(self, config):
        self.name = 'Logger'
        self.config = config


    def create_logger(self, logger_name):
        """Creates a logger and returns it.

        :param logger_name: str name of the logger
        :rtype: A :class:`Logger <Logger>`
        """
        # create logger
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG if self.config['IS_DEV'] or self.config['IS_PREPROD'] else logging.INFO)
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # file handler, scrive i log nel file, prima verifico che esista la cartella e il file
        fl = None
        try:
            fl = logging.FileHandler(self.config['LOGGING_FILE'])
        except FileNotFoundError as e:
            makedirs(path.dirname(self.config['LOGGING_FILE']), exist_ok=True)
            fl = logging.FileHandler(self.config['LOGGING_FILE'])

        fl.setLevel(logging.INFO)
        fl.setFormatter(formatter)
        logger.addHandler(ch)
        logger.addHandler(fl)
        return logger
