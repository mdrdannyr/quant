import config, logging


def init_logger():

    # create logger
    logger = logging.getLogger(config.logger_name)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    fh = logging.FileHandler(config.log_file_path)
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    # Examples
    # logger.debug('Quick zephyrs blow, vexing daft Jim.')
    # logger.info('How quickly daft jumping zebras vex.')
    # logger.warning('Jail zesty vixen who grabbed pay from quack.')
    # logger.error('The five boxing wizards jump quickly.')
