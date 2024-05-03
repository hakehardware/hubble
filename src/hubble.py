import docker

import sys
import signal
import re
import time
import src.constants as constants

from src.logger import logger

from src.nexus_api import NexusAPI
from src.log_parser import LogParser
from datetime import datetime, timedelta
from src.rate_limiter import RateLimiter
from src.helpers import Helpers

class Hubble:
    def __init__(self, config) -> None:
        # create config params
        self.name = config.get('name')
        self.mode = config.get('mode')
        self.nexus_url = config.get('nexus_url')

        # environmental variable for discord alerts
        self.discord_alerts = config.get('discord_alerts')

        # rate limiter
        self.rate_limiter = RateLimiter(limit=4, interval=60)
        
        # docker client
        self.docker_client = docker.from_env()

        # settings
        self.nexus_enabled = False

        # docker container id for node
        self.docker_container_id = None
        self.image_version = None

    # Initialize Nexus API if API URL provided
    def initialize_nexus(self) -> None:
        try:
            if self.nexus_url:
                logger.info(f"Connecting to Nexus API ({self.nexus_url})")
                validated = NexusAPI.validate_api_connection(self.nexus_url)

                if validated:
                    self.nexus_enabled = True
                    logger.info('Handshake with Nexus API successful')
                else:
                    logger.warn(f"Handshake with Nexus API Failed. Check Nexus URL & that Nexus is running.")

            else:
                logger.info('Nexus URL not provided. Nexus API will not be used')

        except Exception as e:
            logger.error(f'Error initializing Nexus API: {e}')

    # Register the Node/Farmer with the Nexus DB
    def register_nexus(self) -> None:
        try:
            if self.mode == 'Farmer':
                NexusAPI.insert_farmer(self.nexus_url, self.name)

            elif self.mode == 'Node':
                NexusAPI.insert_node(self.nexus_url, self.name)

        except Exception as e:
            logger.error(f'Error registering with Nexus API: {e}')

    # Initialize the Discord Notifications
    def initialize_discord(self) -> None:
        try:
            if self.discord_alerts.get('general'):
                logger.info("General discord alerts are enabled")

            if self.discord_alerts.get('farmers'):
                logger.info("Farmer discord notifications are enabled")

            if self.discord_alerts.get('nodes'):
                logger.info("Node discord notifications are enabled")

            if self.discord_alerts.get('farms'):
                logger.info("Farm discord notifications are enabled")

            if self.discord_alerts.get('plots'):
                logger.info("Plot discord notifications are enabled")

            if self.discord_alerts.get('plots'):
                logger.info("Reward discord notifications are enabled")

            if self.discord_alerts.get('plots'):
                logger.info("Error discord notifications are enabled")

            self.discord_publish_threshold = self.discord_alerts.get('publish_threshold', 5)


        except Exception as e:
            logger.error(f'Error initializing Discord notifications: {e}')

    # Send Start Up Alert
    def send_start_up_alert(self):
        try:
            title = "Initialization Complete"
            message = f"{self.name} Initialized. Starting Log Stream Monitor."
            alert_type = 'general'

            Helpers.send_discord_notification(self.discord_alerts, title, message, alert_type, self.rate_limiter)

        except Exception as e:
            logger.warn(f'Unable to send start up alert: {e}')

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

    # Monitor Log Stream, Parse Logs into Events, Handle Events
    def log_stream_monitor(self):
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

                            # start_datetime = Helpers.get_prev_date(10, 'minutes')
                            # logger.info(f"Getting logs since {start_datetime}")

                            generator = container.logs(stdout=True, stderr=True, stream=True)

                            for log in generator:
                                log_str = log.decode('utf-8').strip()

                                if log_str == "Error grabbing logs: invalid character 'l' after object key:value pair":
                                    logger.error('Due to how log rotation works, the log stream is broken until you redeploy your container.')

                                if self.mode == 'Farmer':
                                    pattern = r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)\s*(?P<level>\w+)\s*(?P<data>.*)$"
                                    match = re.match(pattern, log_str)

                                    if match:
                                        self.parse_log(
                                            match.group("timestamp"),
                                            match.group("level"),
                                            match.group("data")
                                        )

                                elif self.mode == 'Node':
                                    pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(\w+)\s+(.*)'
                                    match = re.match(pattern, log_str)
                                    
                                    if match:
                                        timestamp, level, data = match.groups()

                                        self.parse_log(
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
            logger.error("Error in Log Stream Monitor:", exc_info=e)

    # Parse Logs into Events
    def parse_log(self, timestamp, level, data) -> None:
        try:
            event = LogParser.get_log_event(self.name, timestamp, level, data)
            if event and event.get('Event Type') != 'Unknown':
                self.handle_event(event)
            elif event.get('Event Type') == 'Unknown':
                pass
            else:
                logger.error('Unable to evaluate event. This happens the parser cannot find a match for a known event.')
                logger.info(f"Unevaluated Data: {data}")

        except Exception as e:
            logger.error("Error evaluating log:", exc_info=e)

    # Handle Events
    def handle_event(self, event):
            if self.nexus_enabled:
                if self.mode == 'Farmer':
                    result = NexusAPI.insert_farmer_event(self.nexus_url, event)
                elif self.mode == 'Node':
                    result = NexusAPI.insert_node_event(self.nexus_url, event)

                if result.get('Inserted Event'):

                    # INSERT FARM
                    if event['Event Type'] == "Farm ID":
                        if self.nexus_enabled:
                            logger.info('Inserting Farm into Nexus API')
                            NexusAPI.insert_farm(self.nexus_url, event)

                    # UPDATE FARM
                    elif event['Event Type'] == "Farm Public Key":
                        if self.nexus_enabled:
                            logger.info('Updating Farm in Nexus API')
                            NexusAPI.update_farm(self.nexus_url, event)
                    # UPDATE FARM
                    elif event['Event Type'] == "Farm Allocated Space":
                        if self.nexus_enabled:
                            logger.info('Updating Farm in Nexus API')
                            NexusAPI.update_farm(self.nexus_url, event)

                    # UPDATE FARM
                    elif event['Event Type'] == 'Farm Directory':
                        if self.nexus_enabled:
                            logger.info('Updating Farm in Nexus API')
                            NexusAPI.update_farm(self.nexus_url, event)

                    # UPDATE FARMER
                    elif event['Event Type'] == 'Starting Workers':
                        # Update Farm Workers
                        if event['Age'] < self.discord_publish_threshold:
                            Helpers.send_discord_notification(self.discord_alerts, 'Starting Workers', f'{self.name} is starting.', 'farmer', self.rate_limiter)
                        if self.nexus_enabled:
                            logger.info('Updating Farmer in Nexus')
                            NexusAPI.update_farmer(self.nexus_url, event)

                    # UPDATE REWARDS
                    elif event['Event Type'] == 'Failed to Send Solution':
                        if event['Age'] < self.discord_publish_threshold:
                            Helpers.send_discord_notification(self.discord_alerts, 'Failed to Send Solution', f'{self.name} farm index {event["Data"]["Farm Index"]} failed to send solution!', 'reward', self.rate_limiter)
                        if self.nexus_enabled:
                            logger.info('Updating Reward Failed to Send Solution')
                            NexusAPI.update_farmer(self.nexus_url, event)

                    # UPDATE FARM AND PLOT
                    elif event['Event Type'] == 'Replotting Complete':
                        if event['Age'] < self.discord_publish_threshold:
                            Helpers.send_discord_notification(self.discord_alerts, 'Replotting Complete', f'{self.name} farm index {event["Data"]["Farm Index"]} Replotting Complete', 'farm', self.rate_limiter)
                        if self.nexus_enabled:
                            logger.info('Updating Farm Status: Replotting Complete')
                            NexusAPI.update_farm(self.nexus_url, event)
                            NexusAPI.insert_plot(self.nexus_url, event)

                    # UPDATE FARM AND PLOT
                    elif event['Event Type'] == 'Replotting Sector':
                        if self.nexus_enabled:
                            logger.info('Updating Farm Status & Plot: Replotting Sector')
                            NexusAPI.update_farm(self.nexus_url, event)
                            NexusAPI.insert_plot(self.nexus_url, event)

                    # UPDATE FARMER
                    elif event['Event Type'] == 'Synchronizing Piece Cache':
                        if self.nexus_enabled:
                            logger.info('Updating Farmer in Nexus')
                            NexusAPI.update_farmer(self.nexus_url, event)

                    # INSERT REWARD
                    elif event['Event Type'] == 'Reward':
                        if event['Age'] < self.discord_publish_threshold:
                            Helpers.send_discord_notification(self.discord_alerts, 'Reward', f'{self.name} farm index {event["Data"]["Farm Index"]} Received a Reward', 'reward', self.rate_limiter)

                        if self.nexus_enabled:
                            NexusAPI.insert_reward(self.nexus_url, event)

                    # UPDATE FARMER
                    elif event['Event Type'] == 'Finished Piece Cache Sync':
                        if self.nexus_enabled:
                            logger.info('Updating Farmer in Nexus')
                            NexusAPI.update_farmer(self.nexus_url, event)

                    # elif event['Event Type'] == 'Plotting Resumed':
                    #     # Update Farm Status
                    #     if self.api:
                    #         pass # self.database_api.update_farmer_status(event)

                    # elif event['Event Type'] == 'Plotting Paused':
                    #     # Update Farm Status
                    #     if self.api:
                    #         pass # self.database_api.update_farmer_status(event)

                    elif event['Event Type'] == 'Piece Cache Sync':
                        if self.nexus_enabled:
                            logger.info('Updating Farmer in Nexus')
                            NexusAPI.update_farmer(self.nexus_url, event)

                    elif event['Event Type'] == 'Plotting Sector':
                        if self.nexus_enabled:
                            logger.info('Updating Farm Status & Plot: Replotting Sector')
                            NexusAPI.update_farm(self.nexus_url, event)
                            NexusAPI.insert_plot(self.nexus_url, event)

                    elif event['Event Type'] == 'Plotting Complete':
                        if event['Age'] < self.discord_publish_threshold:
                            Helpers.send_discord_notification(self.discord_alerts, 'Plotting Complete', f'{self.name} farm index {event["Data"]["Farm Index"]} Replotting Complete', 'plot', self.rate_limiter)

                        if self.nexus_enabled:
                            logger.info('Updating Farm Status & Plot: Replotting Sector')
                            NexusAPI.update_farm(self.nexus_url, event)
                            NexusAPI.insert_plot(self.nexus_url, event)

                    elif event['Event Type'] == 'Idle Node':
                        if self.nexus_enabled:
                            logger.info('Inserting Consensus')
                            NexusAPI.insert_consensus(self.nexus_url, event)

                    elif event['Event Type'] == 'Claimed Vote':
                        if event['Age'] < self.discord_publish_threshold:
                            Helpers.send_discord_notification(self.discord_alerts, 'Claimed Vote', f'{self.name} ({self.mode}) claimed vote at slot {event["Data"]["Slot"]} for a reward.', 'claim', self.rate_limiter)

                        if self.nexus_enabled:
                            logger.info('Inserting Claim: Vote')
                            NexusAPI.insert_claim(self.nexus_url, event)

                    elif event['Event Type'] == 'Claimed Block':
                        if event['Age'] < self.discord_publish_threshold:
                            Helpers.send_discord_notification(self.discord_alerts, 'Claimed Block', f'{self.name} ({self.mode}) claimed block at slot {event["Data"]["Slot"]} for a reward.', 'claim', self.rate_limiter)

                        if self.nexus_enabled:
                            logger.info('Inserting Claim: Block')
                            NexusAPI.insert_claim(self.nexus_url, event)

                else:
                    pass

    # ... Run
    def run(self) -> None:
        logger.info(f'Initializing hubble {constants.VERSIONS["hubble"]} in {self.mode} mode.')

        # Attempt to initialize Nexus API
        self.initialize_nexus()

        # Register the Node/Farmer with the Nexus DB
        if self.nexus_enabled:
            self.register_nexus()

        # Initialize the Discord Notifications
        self.initialize_discord()

        # Get Container information
        self.get_container()

        # Verify versions
        self.check_version()

        # Send Discord Alert regarding Startup
        self.send_start_up_alert()

        # Start Log Stream Monitor
        self.log_stream_monitor()
