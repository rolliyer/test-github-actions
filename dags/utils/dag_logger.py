import logging

def get_logger(name, level=logging.INFO):
    """
    Initializes basic logging configuration and returns a logger.

    Args:
        name (str): The name for the logger, typically __name__.
        level (int): The logging level, e.g., logging.INFO.

    Returns:
        A configured logger instance.
    """
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(name)
