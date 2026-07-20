import logging

logging.basicConfig(filename="newfile.log",
format='%(asctime)s %(levelname)s: %(message)s',
filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logger.debug("Harmless debug message")
logger.info("Just an information")
logger.warning("Its a warning")
logger.error("Did you try to divide by zero?")
logger.critical("Internet is down")
