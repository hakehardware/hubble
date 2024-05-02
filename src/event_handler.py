# The Event Handler 
from src.logger import logger
from src.nexus_api import NexusAPI
from src.helpers import Helpers

import src.constants as constants

class EventHandler:

    @staticmethod
    def handle_event(event):
        try:
            logger.info(event)
            publish_threshold = constants.PUBLISH_THRESHOLD
            result = NexusAPI.insert_event(self.base_url, event)
            

        except Exception as e:
            logger.error("Error in event handler:", exc_info=e)