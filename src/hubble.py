import docker

from src.logger import logger
import src.constants as constants

class Hubble:
    def __init__(self, config) -> None:
        self.config = config

        # docker container id for node
        self.node_container_id = None

        # docker container id for farmer
        self.farmer_container_id = None

        # environmental variable for discord alerts
        self.discord_webhook = None

        # docker client
        self.docker_client = docker.from_env()

    def run(self) -> None:
        logger.info(f'Initializing hubble {constants.VERSIONS["hubble"]}')