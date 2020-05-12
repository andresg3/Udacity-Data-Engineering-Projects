import logging
import logging.config
from pathlib import Path

logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

logger.debug("I am a TEST separate Logger")
