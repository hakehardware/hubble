import docker
import os
import sys
import signal
import re
import time
import src.constants as constants

from src.logger import logger
from src.discord_api import DiscordAPI
from src.log_parser import LogParser


class Hubble:
    def __init__(self, config) -> None:
        self.config = config

        # docker container id for node
        self.docker_container_id = None
        self.image_version = None

        # environmental variable for discord alerts
        self.discord_webhook = os.getenv('DISCORD_WEBHOOK')

        # docker client
        self.docker_client = docker.from_env()

        # settings
        self.api = False
        self.discord = False

    def send_discord_notification(self, title, message) -> None:
        DiscordAPI.send_discord_message(self.discord_webhook, title, message)

    def get_container(self) -> None:
        try:
            containers = self.docker_client.containers.list(all=True)

            for container in containers:
                if self.config['mode'] == 'Farmer':
                    if 'subspace/farmer' in container.image.tags[0]:
                        self.docker_container_id = container.id
                        logger.info(f'Found Farmer with ID of {self.docker_container_id}.')

                        self.image_version = container.image.labels["org.opencontainers.image.version"]
                        logger.info(f'Farmer Version: {self.image_version}')

                elif self.config['mode'] == 'Node':
                    if 'subspace/node' in container.image.tags[0]:
                        self.docker_container_id = container.id
                        logger.info(f'Found Node with ID of {self.docker_container_id}.')

                        self.image_version = container.image.labels["org.opencontainers.image.version"]
                        logger.info(f'Node Version: {self.image_version}')

            if not self.docker_container_id or not self.image_version:
                logger.error('Unable to find the container. Are you sure it is running?')

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            logger.error('Unable to get container. Exiting.')
            sys.exit(1)

    def check_version(self) -> None:
        try:
            if self.config["mode"] == 'Node':
                logger.info('Checking Node version compatibility')
                if self.image_version != constants.VERSIONS["Node Version"]:
                    logger.warn(f'You are running {self.image_version}. For the best experience use {constants.VERSIONS["Node Version"]}')
                else:
                    logger.info('Node version check passed.')

            if self.config["mode"] == 'Farmer':
                logger.info('Checking Farmer version compatibility')
                if self.image_version != constants.VERSIONS["Farmer Version"]:
                    logger.warn(f'You are running {self.image_version}. For the best experience use {constants.VERSIONS["Farmer Version"]}')
                else:
                    logger.info('Farmer version check passed.')

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            logger.warn('Unable to verify version, Cosmos may not work as intended.')

    def start_log_stream(self) -> None:
        logger.info(f'Starting log stream for {self.config["mode"]} {self.config["name"]}...')
        try:
            container = self.docker_client.containers.get(self.docker_container_id)

            if container:
                logger.info(f'Found Container with ID ending in {self.docker_container_id[:10]}')
                signal.signal(signal.SIGINT, self.signal_handler)

                while True:
                    try:
                        container.reload()
                        if container.status == 'running':
                            generator = container.logs(stdout=True, stderr=True, stream=True)
                            for log in generator:
                                log_str = log.decode('utf-8').strip()
                                pattern = r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z)\s*(?P<level>\w+)\s*(?P<data>.*)$"
                                match = re.match(pattern, log_str)

                                if match:
                                    self.evaluate_log(
                                        match.group("timestamp"),
                                        match.group("level"),
                                        match.group("data")
                                    )
                        else:
                            logger.warn(f'Docker Status: {container.status}')
                            logger.warn('Container does not seem to be running. Sleeping 10 seconds before checking again...')
                            time.sleep(10)


                    except Exception as e:
                        logger.error("Error in task:", exc_info=e)
                        time.sleep(1)

        except Exception as e:
            logger.error("Error in task:", exc_info=e)

    def signal_handler(self, sig, frame):
        print('SIGINT Received, shutting down stream...')
        # Perform any cleanup actions here if needed
        sys.exit(0)

    def evaluate_log(self, timestamp, level, data):
        event = LogParser.get_log_event(self.config['name'], timestamp, level, data)
        publish_threshold = constants.PUBLISH_THRESHOLD

        if level == 'ERROR':
            if event['Age'] < publish_threshold:
                self.send_discord_notification('Error', f'{self.config["name"]} received an error: {data}')

        if event['Event Type'] != 'Unknown':
            logger.info(f'{event}')

            if event['Event Type'] == "Farm ID":
                # Update Farm ID
                if self.api:
                    pass # self.database_api.update_farm_id(event)

            elif event['Event Type'] == "Farm Public Key":
                # Update Farm Public Key
                if self.api:
                    pass # self.database_api.update_farm_pub_key(event)

            elif event['Event Type'] == "Farm Allocated Space":
                # Update Farm Allocated Space
                if self.api:
                    pass # self.database_api.update_farm_alloc_space(event)

            elif event['Event Type'] == 'Farm Directory':
                # Update Farm Directory
                if self.api:
                    pass # self.database_api.update_farm_directory(event)

            elif event['Event Type'] == 'Starting Workers':
                # Update Farm Workers
                if event['Age'] < publish_threshold:
                    self.send_discord_notification('Starting Workers', f'{self.config["name"]} is starting.')
                if self.api:
                    pass # self.database_api.update_farm_workers(event)

            elif event['Event Type'] == 'Failed to Send Solution':
                # Update Rewards for Failed Result
                if event['Age'] < publish_threshold:
                    self.send_discord_notification('Failed to Send Solution', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} failed to send solution!')
                if self.api:
                    pass # self.database_api.update_rewards(event)

            elif event['Event Type'] == 'Replotting Complete':
                # Update Farm Status
                if event['Age'] < publish_threshold:
                    self.send_discord_notification('Replotting Complete', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} Replotting Complete')

                if self.api:
                    pass # self.database_api.update_farm_status(event)

            elif event['Event Type'] == 'Replotting Sector':
                # Update Plots for Replotted Sector
                if self.api:
                    pass # self.database_api.update_plotting(event, 1)

            elif event['Event Type'] == 'Synchronizing Piece Cache':
                # Update Farmer Status
                if self.api:
                    pass # self.database_api.update_piece_cache_status(event)

            elif event['Event Type'] == 'Reward':
                # Update Rewards for Success Result
                if event['Age'] < publish_threshold:
                    self.send_discord_notification('Reward', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} Received a Reward')

                if self.api:
                    pass # self.database_api.update_rewards(event)

            elif event['Event Type'] == 'Finished Piece Cache Sync':
                # Update Farmer Status
                if self.api:
                    pass # self.database_api.update_piece_cache_status(event)

            elif event['Event Type'] == 'Plotting Resumed':
                # Update Farm Status
                if self.api:
                    pass # self.database_api.update_farmer_status(event)

            elif event['Event Type'] == 'Plotting Paused':
                # Update Farm Status
                if self.api:
                    pass # self.database_api.update_farmer_status(event)

            elif event['Event Type'] == 'Piece Cache Sync':
                # Update Farmer Status
                if self.api:
                    pass # self.database_api.update_piece_cache_sync(event)

            elif event['Event Type'] == 'Plotting Sector':
                # Update Plots for Plotted Sector
                if self.api:
                    pass # self.database_api.update_plotting(event, 0)

            elif event['Event Type'] == 'Plotting Complete':
                # Update Farm Status
                if event['Age'] < publish_threshold:
                    self.send_discord_notification('Plotting Complete', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} Replotting Complete')

                if self.api:
                    pass # self.database_api.update_farm_status(event)   

    def run(self) -> None:
        logger.info(f'Initializing hubble {constants.VERSIONS["hubble"]} in {self.config["mode"]} mode.')

        # check if hubble should connect to the API
        if 'api_url' in self.config:
            self.api = True
            logger.info('Connection to API')
        else:
            logger.info('No API URL found. Skipping API Connection')

        # check if hubble should send discord messages
        if self.discord_webhook:
            self.discord = True
            logger.info('Sending discord start notification')
            self.send_discord_notification('Hubble Started', f'Hubble has been started for {self.config["name"]}')
        else:
            logger.info('No Discord Webhook found. Not using Discord Notifications.')

        # get container information
        self.get_container()

        # check version compatibility
        self.check_version()

        # start log stream
        self.start_log_stream()