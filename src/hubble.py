import docker

import sys
import signal
import re
import time
import src.constants as constants
import json

from src.logger import logger

from src.spaceport_api import SpaceportAPI
from src.log_parser import LogParser
from datetime import datetime, timedelta
from src.rate_limiter import RateLimiter
from src.helpers import Helpers

class Hubble:
    def __init__(self, config) -> None:
        # create config params
        self.config = config

        # docker container id for node
        self.docker_data = {
            'Container ID': None,
            'Image Version': None,
            'Container Status': None,
            'Container Started At': None,
            'Container IP': None
        }

        # rate limiter
        self.rate_limiter = RateLimiter(limit=4, interval=60)
        
        # docker client
        self.docker_client = docker.from_env()


    # Get the container info from Docker
    def get_container(self) -> None:
        try:
            logger.info("Getting container")
            containers = self.docker_client.containers.list(all=True)
            match = None

            for container in containers:
                if self.config.get('mode') == 'Farmer':
                    if 'subspace/farmer' in container.image.tags[0]:
                        match = container

                elif self.config.get('mode') == 'Node':
                    if 'subspace/node' in container.image.tags[0]:
                        match = container

            if match:
                network_mode = match.attrs.get('HostConfig').get('NetworkMode')
                self.docker_data['Container ID'] = match.id
                self.docker_data['Image Version'] = match.image.labels["org.opencontainers.image.version"]
                self.docker_data['Container Status'] = match.status
                self.docker_data['Container Started At'] = match.attrs.get('State').get('StartedAt')
                self.docker_data['Container IP'] = match.attrs.get('NetworkSettings').get('Networks')[network_mode].get('IPAddress')
                logger.info(f"Docker: {self.docker_data}")

            else:
                logger.error('Unable to find the container. Are you sure it is exists and that the mode is set correctly in the config?')
                sys.exit(1)

        except Exception as e:
            logger.error(f'Unable to get container: {e}')
            sys.exit(1)

    def register_node(self) -> None:
        try:
            nodes = SpaceportAPI.get_nodes(self.config.get('spaceport_url'))

            if nodes == None:
                logger.error('Failed to register node. Exiting')
                sys.exit(1)

            logger.info(f"Found {len(nodes)} Node(s). Checking if current Node is already registered")

            node_exists = False
            for node in nodes:
                if node.get('name') == self.config.get('name'):
                    node_exists = True
                    break

            if node_exists:
                logger.info('Found Node. Updating Node registration')
                SpaceportAPI.update_node(self.config.get('spaceport_url'), {
                    'name': self.config.get('name'),
                    'status': 'Initializing',
                    'active': True,
                    'hostIp': self.config.get('host_ip'),
                    'containerIp': self.docker_data.get('Container IP'),
                    'containerStatus': self.docker_data.get('Container Status'),
                    'version': self.docker_data.get('Image Version'),
                    'containerStartedAt': self.docker_data.get('Container Started At')
                })

            else:
                logger.info('Registering Node with Spaceport API')
                SpaceportAPI.insert_node(self.config.get('spaceport_url'), {
                    'name': self.config.get('name'),
                    'status': 'Initializing',
                    'active': True,
                    'hostIp': self.config.get('host_ip'),
                    'containerIp': self.docker_data.get('Container IP'),
                    'containerStatus': self.docker_data.get('Container Status'),
                    'version': self.docker_data.get('Image Version'),
                    'containerStartedAt': self.docker_data.get('Container Started At')
                })

        except Exception as e:
            logger.error(f'Error registering Node with Spaceport API: {e}')
            sys.exit(1)

    def register_farmer(self) -> None:
        pass

    # Check version compatibility
    def check_version(self) -> None:
        try:
            if self.config['mode'] == 'Node':
                logger.info('Checking Node version compatibility')
                if self.docker_data['Image Version'] != constants.VERSIONS["Node Version"]:
                    logger.warn(f"You are running {self.docker_data['Image Version']}. For the best experience use {constants.VERSIONS['Node Version']}")
                else:
                    logger.info('Node version check passed.')

            if self.config['mode'] == 'Farmer':
                logger.info('Checking Farmer version compatibility')
                if self.docker_data['Image Version'] != constants.VERSIONS["Farmer Version"]:
                    logger.warn(f"You are running {self.docker_data['Image Version']}. For the best experience use {constants.VERSIONS['Farmer Version']}")
                else:
                    logger.info('Farmer version check passed.')

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.warn(f'Unable to verify version, Hubble may not work as intended: {e}')

    # # Initialize the Discord Notifications
    # def initialize_discord(self) -> None:
    #     try:
    #         if self.discord_alerts.get('general'):
    #             logger.info("General discord alerts are enabled")

    #         if self.discord_alerts.get('farmers'):
    #             logger.info("Farmer discord notifications are enabled")

    #         if self.discord_alerts.get('nodes'):
    #             logger.info("Node discord notifications are enabled")

    #         if self.discord_alerts.get('farms'):
    #             logger.info("Farm discord notifications are enabled")

    #         if self.discord_alerts.get('plots'):
    #             logger.info("Plot discord notifications are enabled")

    #         if self.discord_alerts.get('plots'):
    #             logger.info("Reward discord notifications are enabled")

    #         if self.discord_alerts.get('plots'):
    #             logger.info("Error discord notifications are enabled")

    #         self.discord_publish_threshold = self.discord_alerts.get('publish_threshold', 5)


    #     except Exception as e:
    #         logger.error(f'Error initializing Discord notifications: {e}')

    # # Send Start Up Alert
    # def send_start_up_alert(self):
    #     try:
    #         title = "Initialization Complete"
    #         message = f"{self.config['name']} Initialized. Starting Log Stream Monitor."
    #         alert_type = 'general'

    #         Helpers.send_discord_notification(self.discord_alerts, title, message, alert_type, self.rate_limiter)

    #     except Exception as e:
    #         logger.warn(f'Unable to send start up alert: {e}')

    # Signal Handler for Stopping Stream
    def signal_handler(self, sig, frame) -> None:
        print('SIGINT Received, shutting down stream...')
        # Perform any cleanup actions here if needed
        sys.exit(0)

    # Monitor Log Stream, Parse Logs into Events, Handle Events
    def log_stream_monitor(self):
        try:
            logger.info(f"Starting log stream for {self.config['mode']} {self.config['name']}")
            container = self.docker_client.containers.get(self.docker_data['Container ID'])

            if container:
                logger.info(f"Connected to container")
                signal.signal(signal.SIGINT, self.signal_handler)

                while True:
                    try:
                        # Container status is cached so we must reload it
                        container.reload()
                        if container.status == 'running':

                            start_datetime = Helpers.get_prev_date(1, 'minutes')
                            logger.info(f"Getting logs since {start_datetime}")

                            generator = container.logs(since=start_datetime, stdout=True, stderr=True, stream=True)

                            for log in generator:
                                log_str = log.decode('utf-8').strip()

                                if log_str == "Error grabbing logs: invalid character 'l' after object key:value pair":
                                    logger.error("Due to how log rotation works, the log stream is broken until you redeploy your container.")

                                if self.config['mode'] == 'Farmer':
                                    pattern = r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)\s*(?P<level>\w+)\s*(?P<data>.*)$"
                                    match = re.match(pattern, log_str)

                                    if match:
                                        self.parse_log(
                                            match.group("timestamp"),
                                            match.group("level"),
                                            match.group("data")
                                        )

                                elif self.config['mode'] == 'Node':
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
                            logger.warn(f"Container currently has a status of {container.status}. Sleeping 10 seconds before checking again...")
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
            event = LogParser.get_log_event(self.config['name'], timestamp, level, data)
            if event and event.get('Event Type') != 'Unknown':
                self.handle_event(event)

            elif event.get('Event Type') == 'Unknown':
                pass
            else:
                logger.error('Unable to evaluate event. This happens the parser cannot find a match for a known event.')
                logger.info(f"Unevaluated Data: {data}")

        except Exception as e:
            logger.error("Error evaluating log:", exc_info=e)


    def handle_event(self, event):
        base_url = self.config.get('spaceport_url')
        if self.config['mode'] == 'Farmer':
            inserted = SpaceportAPI.insert_farmer_event(base_url, event)
        elif self.config['mode'] == 'Node':
            inserted = SpaceportAPI.insert_node_event(base_url, event)
        
        if inserted and self.config.get('mode') == 'Node':
            logger.info(f"Inserting {event['Event Type']}")

            if event['Event Type'] == 'Idle Node':
                SpaceportAPI.insert_consensus(base_url, event)
                SpaceportAPI.update_node(base_url, {
                    'name': event.get('Node Name'),
                    'status': 'Idle'
                })

            elif event['Event Type'] in ['Vote', 'Block']:
                SpaceportAPI.insert_claim(base_url, event)

                    # elif event['Event Type'] == 'Claimed Vote':
                    #     if event['Age'] < self.discord_publish_threshold:
                    #         Helpers.send_discord_notification(self.discord_alerts, 'Claimed Vote', f"{self.config['name']} ({self.config['mode']}) claimed vote at slot {event['Data']['Slot']} for a reward.", 'claim', self.rate_limiter)

                    #     if self.nexus_enabled:
                    #         logger.info('Inserting Claim: Vote')
                    #         SpaceportAPI.insert_claim(self.nexus_url, event)

                    # elif event['Event Type'] == 'Claimed Block':
                    #     if event['Age'] < self.discord_publish_threshold:
                    #         Helpers.send_discord_notification(self.discord_alerts, 'Claimed Block', f"{self.config['name']} ({self.config['mode']}) claimed block at slot {event['Data']['Slot']} for a reward.", 'claim', self.rate_limiter)

                    #     if self.nexus_enabled:
                    #         logger.info('Inserting Claim: Block')
                    #         SpaceportAPI.insert_claim(self.nexus_url, event)

                #     # INSERT FARM
                #     if event['Event Type'] == "Farm ID":
                #         if self.nexus_enabled:
                #             logger.info('Inserting Farm into Nexus API')
                #             SpaceportAPI.insert_farm(self.nexus_url, event)

                #     # UPDATE FARM
                #     elif event['Event Type'] == "Farm Public Key":
                #         if self.nexus_enabled:
                #             logger.info('Updating Farm in Nexus API')
                #             SpaceportAPI.update_farm(self.nexus_url, event)
                #     # UPDATE FARM
                #     elif event['Event Type'] == "Farm Allocated Space":
                #         if self.nexus_enabled:
                #             logger.info('Updating Farm in Nexus API')
                #             SpaceportAPI.update_farm(self.nexus_url, event)

                #     # UPDATE FARM
                #     elif event['Event Type'] == 'Farm Directory':
                #         if self.nexus_enabled:
                #             logger.info('Updating Farm in Nexus API')
                #             SpaceportAPI.update_farm(self.nexus_url, event)

                #     # UPDATE FARMER
                #     elif event['Event Type'] == 'Starting Workers':
                #         # Update Farm Workers
                #         if event['Age'] < self.discord_publish_threshold:
                #             Helpers.send_discord_notification(self.discord_alerts, "Starting Workers', f'{self.name} is starting.", 'farmer', self.rate_limiter)
                #         if self.nexus_enabled:
                #             logger.info('Updating Farmer in Nexus')
                #             SpaceportAPI.update_farmer(self.nexus_url, event)

                #     # UPDATE REWARDS
                #     elif event['Event Type'] == 'Failed to Send Solution':
                #         if event['Age'] < self.discord_publish_threshold:
                #             Helpers.send_discord_notification(self.discord_alerts, 'Failed to Send Solution', f"{self.name} farm index {event['Data']['Farm Index']} failed to send solution!", 'reward', self.rate_limiter)
                #         if self.nexus_enabled:
                #             logger.info('Updating Reward Failed to Send Solution')
                #             SpaceportAPI.update_farmer(self.nexus_url, event)

                #     # UPDATE FARM AND PLOT
                #     elif event['Event Type'] == 'Replotting Complete':
                #         if event['Age'] < self.discord_publish_threshold:
                #             Helpers.send_discord_notification(self.discord_alerts, 'Replotting Complete', f"{self.name} farm index {event['Data']['Farm Index']} Replotting Complete", 'farm', self.rate_limiter)
                #         if self.nexus_enabled:
                #             logger.info('Updating Farm Status: Replotting Complete')
                #             SpaceportAPI.update_farm(self.nexus_url, event)
                #             SpaceportAPI.insert_plot(self.nexus_url, event)

                #     # UPDATE FARM AND PLOT
                #     elif event['Event Type'] == 'Replotting Sector':
                #         if self.nexus_enabled:
                #             logger.info('Updating Farm Status & Plot: Replotting Sector')
                #             SpaceportAPI.update_farm(self.nexus_url, event)
                #             SpaceportAPI.insert_plot(self.nexus_url, event)

                #     # UPDATE FARMER
                #     elif event['Event Type'] == 'Synchronizing Piece Cache':
                #         if self.nexus_enabled:
                #             logger.info('Updating Farmer in Nexus')
                #             SpaceportAPI.update_farmer(self.nexus_url, event)

                #     # INSERT REWARD
                #     elif event['Event Type'] == 'Reward':
                #         if event['Age'] < self.discord_publish_threshold:
                #             Helpers.send_discord_notification(self.discord_alerts, 'Reward', f"{self.name} farm index {event['Data']['Farm Index']} Received a Reward", 'reward', self.rate_limiter)

                #         if self.nexus_enabled:
                #             SpaceportAPI.insert_reward(self.nexus_url, event)

                #     # UPDATE FARMER
                #     elif event['Event Type'] == 'Finished Piece Cache Sync':
                #         if self.nexus_enabled:
                #             logger.info('Updating Farmer in Nexus')
                #             SpaceportAPI.update_farmer(self.nexus_url, event)

                #     # elif event['Event Type'] == 'Plotting Resumed':
                #     #     # Update Farm Status
                #     #     if self.api:
                #     #         pass # self.database_api.update_farmer_status(event)

                #     # elif event['Event Type'] == 'Plotting Paused':
                #     #     # Update Farm Status
                #     #     if self.api:
                #     #         pass # self.database_api.update_farmer_status(event)

                #     elif event['Event Type'] == 'Piece Cache Sync':
                #         if self.nexus_enabled:
                #             logger.info('Updating Farmer in Nexus')
                #             SpaceportAPI.update_farmer(self.nexus_url, event)

                #     elif event['Event Type'] == 'Plotting Sector':
                #         if self.nexus_enabled:
                #             logger.info('Updating Farm Status & Plot: Replotting Sector')
                #             SpaceportAPI.update_farm(self.nexus_url, event)
                #             SpaceportAPI.insert_plot(self.nexus_url, event)

                #     elif event['Event Type'] == 'Plotting Complete':
                #         if event['Age'] < self.discord_publish_threshold:
                #             Helpers.send_discord_notification(self.discord_alerts, 'Plotting Complete', f"{self.config['name']} farm index {event['Data']['Farm Index']} Replotting Complete", 'plot', self.rate_limiter)

                #         if self.nexus_enabled:
                #             logger.info('Updating Farm Status & Plot: Replotting Sector')
                #             SpaceportAPI.update_farm(self.nexus_url, event)
                #             SpaceportAPI.insert_plot(self.nexus_url, event)

                #     elif event['Event Type'] == 'Idle Node':
                #         if self.nexus_enabled:
                #             logger.info('Inserting Consensus')
                #             SpaceportAPI.insert_consensus(self.nexus_url, event)

                #     elif event['Event Type'] == 'Claimed Vote':
                #         if event['Age'] < self.discord_publish_threshold:
                #             Helpers.send_discord_notification(self.discord_alerts, 'Claimed Vote', f"{self.config['name']} ({self.config['mode']}) claimed vote at slot {event['Data']['Slot']} for a reward.", 'claim', self.rate_limiter)

                #         if self.nexus_enabled:
                #             logger.info('Inserting Claim: Vote')
                #             SpaceportAPI.insert_claim(self.nexus_url, event)

                #     elif event['Event Type'] == 'Claimed Block':
                #         if event['Age'] < self.discord_publish_threshold:
                #             Helpers.send_discord_notification(self.discord_alerts, 'Claimed Block', f"{self.config['name']} ({self.config['mode']}) claimed block at slot {event['Data']['Slot']} for a reward.", 'claim', self.rate_limiter)

                #         if self.nexus_enabled:
                #             logger.info('Inserting Claim: Block')
                #             SpaceportAPI.insert_claim(self.nexus_url, event)

                # else:
                #     pass

    # ... Run
    def run(self) -> None:
        logger.info(f"Initializing hubble {constants.VERSIONS['hubble']} in {self.config.get('mode')} mode.")

        # Get Container information
        self.get_container()

        # Register Farmer/Node
        if self.config.get('mode') == 'Node':
            self.register_node()
        elif self.config.get('mode') == 'Farmer':
            self.register_farmer()

        # Verify versions
        self.check_version()

        # # Start Log Stream Monitor
        self.log_stream_monitor()
