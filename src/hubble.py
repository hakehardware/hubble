import docker
import os
import sys
import signal
import re
import time
import src.constants as constants
import json

from src.logger import logger
from src.discord_api import DiscordAPI
from src.log_parser import LogParser
from datetime import datetime, timedelta

class Hubble:
    def __init__(self, config) -> None:
        self.config = config

        # docker container id for node
        self.docker_container_id = None
        self.image_version = None

        # environmental variable for discord alerts
        self.discord_alert = os.getenv('DISCORD_ALERT')
        self.discord_error = os.getenv('DISCORD_ERROR')

        # docker client
        self.docker_client = docker.from_env()

        # docker rate limit prevention (1 message per minute)
        self.discord_last_sent = datetime.now() - timedelta(minutes=1)

        # settings
        self.api = False
        self.discord = False

    def send_discord_notification(self, title, message, notification_type) -> None:
        try:
            current_time = datetime.now()

            if current_time - self.discord_last_sent >= timedelta(minutes=1):
                if notification_type == 'ERROR':
                    url = self.discord_error
                elif notification_type == 'ALERT':
                    url = self.discord_alert
                else:
                    url = self.discord_alert

                DiscordAPI.send_discord_message(url, title, message, notification_type)
            else:
                logger.warn('To prevent Discord rate limits, the last message was supressed.')
                DiscordAPI.send_discord_message(url, 'Rate Limited', 'Messages have been supressed due to rate limits. Please check logs.', 'ERROR')

        except Exception as e:
            logger.warn(f'Unable to send discord message: {e}')

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
            logger.warn(f'Unable to verify version, Cosmos may not work as intended: {e}')

    def start_log_stream(self) -> None:
        try:
            logger.info(f'Starting log stream for {self.config["mode"]} {self.config["name"]}...')
            container = self.docker_client.containers.get(self.docker_container_id)

            # For the Node we don't need to go too far back since 
            if self.config["mode"] == "Node":
                start_time = datetime.now() - timedelta(days=constants.NODE_BACKFILL)
            elif self.config["mode"] == "Farmer":
                start_time = datetime.now() - timedelta(days=constants.FARMER_BACKFILL)

            if container:
                logger.info(f'Connected to container')
                signal.signal(signal.SIGINT, self.signal_handler)

                while True:
                    try:
                        # Container status is cached so we must reload it
                        container.reload()

                        if container.status == 'running':
                            generator = container.logs(since=start_time, stdout=True, stderr=True, stream=True)

                            for log in generator:
                                log_str = log.decode('utf-8').strip()

                                if log_str == "Error grabbing logs: invalid character 'l' after object key:value pair":
                                    logger.error('Due to how log rotation works, the log stream is broken until you redeploy your container.')

                                if self.config['mode'] == 'Farmer':
                                    pattern = r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)\s*(?P<level>\w+)\s*(?P<data>.*)$"
                                    match = re.match(pattern, log_str)

                                    if match:
                                        self.evaluate_log(
                                            match.group("timestamp"),
                                            match.group("level"),
                                            match.group("data")
                                        )

                                elif self.config['mode'] == 'Node':
                                    pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(\w+)\s+(.*)'
                                    match = re.match(pattern, log_str)
                                    
                                    if match:
                                        timestamp, level, data = match.groups()
                                        logger.info(data)

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

        except Exception as e:
            logger.error("Error in log stream:", exc_info=e)

    def signal_handler(self, sig, frame) -> None:
        print('SIGINT Received, shutting down stream...')
        # Perform any cleanup actions here if needed
        sys.exit(0)

    def evaluate_log(self, timestamp, level, data) -> None:
        try:
            event = LogParser.get_log_event(self.config['name'], timestamp, level, data)
            publish_threshold = constants.PUBLISH_THRESHOLD

            if level == 'ERROR':
                if event['Age'] < publish_threshold:
                    self.send_discord_notification('Error', f'{self.config["name"]} ({self.config["mode"]}) received an error: {data}', 'ERROR')

            if not event:
                logger.error('Unable to evaluate event. This happens the parser cannot find a match for a known event.')
                logger.info(f"Unevaluated Data: {data}")

            elif event['Event Type'] != 'Unknown':
                # logger.info(f'{event}')

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
                        self.send_discord_notification('Starting Workers', f'{self.config["name"]} is starting.', 'ALERT')
                    if self.api:
                        pass # self.database_api.update_farm_workers(event)

                elif event['Event Type'] == 'Failed to Send Solution':
                    # Update Rewards for Failed Result
                    if event['Age'] < publish_threshold:
                        self.send_discord_notification('Failed to Send Solution', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} failed to send solution!', 'ERROR')
                    if self.api:
                        pass # self.database_api.update_rewards(event)

                elif event['Event Type'] == 'Replotting Complete':
                    # Update Farm Status
                    if event['Age'] < publish_threshold:
                        self.send_discord_notification('Replotting Complete', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} Replotting Complete', 'ALERT')

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
                        self.send_discord_notification('Reward', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} Received a Reward', 'ALERT')

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
                        self.send_discord_notification('Plotting Complete', f'{self.config["name"]} farm index {event["Data"]["Farm Index"]} Replotting Complete', 'ALERT')

                    if self.api:
                        pass # self.database_api.update_farm_status(event)  
                elif event['Event Type'] == 'Idle Node':
                    # Update Sync
                    if self.api:
                        pass # self.database_api.update_rewards(event)
                elif event['Event Type'] == 'Claimed Vote':
                    if event['Age'] < publish_threshold:
                        self.send_discord_notification('Claimed Vote', f'{self.config["name"]} ({self.config["mode"]}) claimed vote at slot {event["Data"]["Slot"]} for a reward.', 'ALERT')

                    if self.api:
                        pass # self.database_api.update_rewards(event)
                elif event['Event Type'] == 'Claimed Block':
                    if event['Age'] < publish_threshold:
                        self.send_discord_notification('Claimed Block', f'{self.config["name"]} ({self.config["mode"]}) claimed block at slot {event["Data"]["Slot"]} for a reward.', 'ALERT')

                    if self.api:
                        pass # self.database_api.update_rewards(event)
            else:
                # Since we already display all errors - we only display events that are Unknown that do not have a level of ERROR
                if constants.DISPLAY_UNKNOWN and level != 'ERROR':
                    logger.info(event)
        
        except Exception as e:
            logger.error("Error evaluating log:", exc_info=e)
            sys.exit(1)

    def run(self) -> None:
        logger.info(f'Initializing hubble {constants.VERSIONS["hubble"]} in {self.config["mode"]} mode.')

        # check if hubble should connect to the API
        if 'api_url' in self.config:
            self.api = True
            logger.info('Connection to API')
        else:
            logger.info('No API URL found. Skipping API Connection')

        # check if hubble should send discord messages
        if self.discord_alert and self.discord_error:
            self.discord = True
            logger.info('Sending discord start notification')
            self.send_discord_notification('Hubble Started', f'Hubble has been started for {self.config["name"]}', 'INFO')
        else:
            logger.info('No Discord Webhook found. Not using Discord Notifications.')

        logger.info(f'Events that occurred within the last {constants.PUBLISH_THRESHOLD} minutes will be published')
        if self.config["mode"] == 'Farmer':
            backfill = constants.FARMER_BACKFILL
        elif self.config["mode"] == 'Node':
            backfill = constants.NODE_BACKFILL
        logger.info(f'Hubble will attempt to backfill up to {backfill} days of logs.')

        if constants.DISPLAY_UNKNOWN:
            logger.info(f'Unknown events will be displayed.')
        elif not constants.DISPLAY_UNKNOWN:
            logger.info('Unknown events will not be displayed')

        # get container information
        self.get_container()

        # check version compatibility
        self.check_version()

        # start log stream
        self.start_log_stream()