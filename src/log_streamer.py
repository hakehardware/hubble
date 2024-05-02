import sys
import docker
import signal
import time
import re

import src.constants as constants

from src.logger import logger
from src.helpers import Helpers
from src.log_parser import LogParser
from src.event_handler import EventHandler

class LogStreamMonitor:
    def __init__(self, args) -> None:
        # create config params
        self.name = args.get('name')
        self.mode = args.get('mode')
        self.nexus_url = args.get('nexus_url')

        # docker container id for node
        self.docker_container_id = None
        self.image_version = None

        # docker client
        self.docker_client = docker.from_env()

    # Get the container info from Docker
    def get_container(self) -> None:
        try:
            containers = self.docker_client.containers.list(all=True)
            for container in containers:
                if self.mode == 'Farmer':
                    if 'subspace/farmer' in container.image.tags[0]:
                        self.docker_container_id = container.id
                        logger.info(f'Found Farmer with ID of {self.docker_container_id}.')

                        self.image_version = container.image.labels["org.opencontainers.image.version"]
                        logger.info(f'Farmer Version: {self.image_version}')

                elif self.mode == 'Node':
                    if 'subspace/node' in container.image.tags[0]:
                        self.docker_container_id = container.id
                        logger.info(f'Found Node with ID of {self.docker_container_id}.')

                        self.image_version = container.image.labels["org.opencontainers.image.version"]
                        logger.info(f'Node Version: {self.image_version}')

            if not self.docker_container_id or not self.image_version:
                logger.error('Unable to find the container. Are you sure it is exists and that the mode is set correctly in the config?')
                sys.exit(1)

        except Exception as e:
            logger.error(f'Unable to get container: {e}')
            sys.exit(1)

    # Check version compatibility
    def check_version(self) -> None:
        try:
            if self.mode == 'Node':
                logger.info('Checking Node version compatibility')
                if self.image_version != constants.VERSIONS["Node Version"]:
                    logger.warn(f'You are running {self.image_version}. For the best experience use {constants.VERSIONS["Node Version"]}')
                else:
                    logger.info('Node version check passed.')

            if self.mode == 'Farmer':
                logger.info('Checking Farmer version compatibility')
                if self.image_version != constants.VERSIONS["Farmer Version"]:
                    logger.warn(f'You are running {self.image_version}. For the best experience use {constants.VERSIONS["Farmer Version"]}')
                else:
                    logger.info('Farmer version check passed.')

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.warn(f'Unable to verify version, Hubble may not work as intended: {e}')

    # Signal Handler for Stopping Stream
    def signal_handler(self, sig, frame) -> None:
        print('SIGINT Received, shutting down stream...')
        # Perform any cleanup actions here if needed
        sys.exit(0)

    # Start Log Stream
    def start_log_stream(self) -> None:
        try:
            logger.info(f'Starting log stream for {self.mode} {self.name}')
            container = self.docker_client.containers.get(self.docker_container_id)

            if container:
                logger.info(f'Connected to container')
                signal.signal(signal.SIGINT, self.signal_handler)

                while True:
                    try:
                        # Container status is cached so we must reload it
                        container.reload()
                        if container.status == 'running':
                            generator = container.logs(stdout=True, stderr=True, stream=True)

                            for log in generator:
                                log_str = log.decode('utf-8').strip()

                                if log_str == "Error grabbing logs: invalid character 'l' after object key:value pair":
                                    logger.error('Due to how log rotation works, the log stream is broken until you redeploy your container.')

                                if self.mode == 'Farmer':
                                    pattern = r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)\s*(?P<level>\w+)\s*(?P<data>.*)$"
                                    match = re.match(pattern, log_str)

                                    if match:
                                        self.evaluate_log(
                                            match.group("timestamp"),
                                            match.group("level"),
                                            match.group("data")
                                        )

                                elif self.mode == 'Node':
                                    pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(\w+)\s+(.*)'
                                    match = re.match(pattern, log_str)
                                    
                                    if match:
                                        timestamp, level, data = match.groups()

                                        self.evaluate_log(
                                            timestamp,
                                            level,
                                            data
                                        )
                        else:
                            logger.warn(f'Container currently has a status of {container.status}. Sleeping 10 seconds before checking again...')
                            time.sleep(10)

                    except Exception as e:
                        logger.error("Error in log stream loop:", exc_info=e)
                        time.sleep(1) 

            else:
                logger.error('Unable to get container. Exiting')
                sys.exit(0)

        except Exception as e:
            logger.error("Error in log stream:", exc_info=e)
    
    # Evaluate Log
    def evaluate_log(self, timestamp, level, data) -> None:
        try:
            event = LogParser.get_log_event(self.name, timestamp, level, data)
            if event:
                EventHandler.handle_event(event, self.nexus_url)
            else:
                logger.error('Unable to evaluate event. This happens the parser cannot find a match for a known event.')
                logger.info(f"Unevaluated Data: {data}")

        except Exception as e:
            logger.error("Error evaluating log:", exc_info=e)


    def run(self):
        self.get_container()

        self.check_version()

        self.start_log_stream()