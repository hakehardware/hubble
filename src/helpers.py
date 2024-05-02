import os
import yaml
import datetime

from src.discord_api import DiscordAPI
from src.logger import logger

class Helpers:
    @staticmethod
    def read_yaml_file(file_path):
        logger.info(f'Opening config from {file_path}')
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                return data
            except yaml.YAMLError as e:
                print(f"Error reading YAML file: {e}")
                return None
            
    @staticmethod
    def get_age_of_timestamp(timestamp):
        # Parse the UTC timestamp string to a datetime object
        utc_time = datetime.datetime.fromisoformat(timestamp + "+00:00")

        # Get the current time in UTC
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        # Convert UTC current time to user's local time
        user_timezone = datetime.datetime.now().astimezone().tzinfo
        local_now = now_utc.astimezone(user_timezone)

        # Convert UTC timestamp to user's local time
        local_timestamp = utc_time.astimezone(user_timezone)


        # Calculate the difference in minutes
        time_diff = local_now - local_timestamp
        minutes_diff = round(time_diff.total_seconds() / 60)

        return minutes_diff
    
    @staticmethod
    # Sends a discord notification
    def send_discord_notification(discord_alerts, title, message, alert_type, rate_limiter) -> None:
        try:
            if rate_limiter.can_send_message():

                urls = {
                    'general': discord_alerts.get('general'),
                    'farmer': discord_alerts.get('farmers'),
                    'node': discord_alerts.get('nodes'),
                    'farm': discord_alerts.get('farms'),
                    'plot': discord_alerts.get('plots'),
                    'reward': discord_alerts.get('rewards'),
                    'error': discord_alerts.get('errors')
                }

                alert_enabled = urls.get(alert_type) is not None

                if alert_enabled:
                    alert_url = urls.get(alert_type)
                    logger.info(f"DISCORD ({alert_type}): {title} - {message}")
                    DiscordAPI.send_discord_message(alert_url, message, title, alert_type)

            else:
                logger.warn('Discord Rate limiter hit. Suppressing message!')

        except Exception as e:
            logger.warn(f'Unable to send discord message: {e}')