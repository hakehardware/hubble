import requests
from src.logger import logger
import uuid
import random

class NexusAPI:
    @staticmethod
    def validate_api_connection(base_url) -> bool:
        url = f"{base_url}/hello"
        response = requests.get(url)
        json_data = response.json()

        if json_data.get('message', None) == 'Hi':
            return True
        else:
            return False
        


    @staticmethod
    def insert_farmer(base_url, farmer_name):
        try:
            local_url = f'{base_url}/insert/farmer'

            data = {
                'Farmer Name': farmer_name
            }

            response = requests.post(local_url, json=data)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"NEXUS: {message}")
            else:
                logger.error(f"NEXUS: {message}")

        except Exception as e:
            logger.error(f'Error inserting farmer via Nexus API')

    @staticmethod
    def insert_farm(base_url, event):
        try:
            local_url = f'{base_url}/insert/farm'
            response = requests.post(local_url, json=event)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"API: {message}")
            else:
                logger.error(f"API: {message}")

        except Exception as e:
            logger.error(f'Error inserting farm via Nexus API')

    @staticmethod
    def insert_event(base_url, event):
        try:
            local_url = f'{base_url}/insert/event'
            response = requests.post(local_url, json=event)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"NEXUS: {message}")
                return json_data
            else:
                logger.error(f"NEXUS: {message}")
                return False

        except Exception as e:
            logger.error(f'NEXUS: Error inserting event via Nexus API')
            return False

    @staticmethod
    def insert_reward(base_url, event):
        try:
            local_url = f'{base_url}/insert/reward'
            response = requests.post(local_url, json=event)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"NEXUS: {message}")
                return True
            else:
                logger.error(f"NEXUS: {message}")
                return False

        except Exception as e:
            logger.error(f'NEXUS: Error inserting reward via Nexus API')
            return False

    @staticmethod
    def insert_plot(base_url, event):
        try:
            logger.info(event)
            local_url = f'{base_url}/insert/plot'
            response = requests.post(local_url, json=event)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"NEXUS: {message}")
                return True
            else:
                logger.error(f"NEXUS: {message}")
                return False

        except Exception as e:
            logger.error(f'NEXUS: Error inserting plot via Nexus API')
            return False

    @staticmethod
    def update_farm(base_url, event):
        try:
            local_url = f'{base_url}/update/farm'

            response = requests.post(local_url, json=event)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"API: {message}")
            else:
                logger.error(f"API: {message}")

        except Exception as e:
            logger.error(f'Error updating farm via Nexus API')

    @staticmethod
    def update_farmer(base_url, event):
        try:
            local_url = f'{base_url}/update/farmer'

            response = requests.post(local_url, json=event)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"API: {message}")
            else:
                logger.error(f"API: {message}")

        except Exception as e:
            logger.error(f'Error updating farmer via Nexus API')

    @staticmethod
    def delete_all_plots(base_url):
        try:
            logger.info('Deleting all plots')
            local_url = f"{base_url}/delete/plots/all"
            response = requests.post(local_url)
            logger.info(response.json())
        except Exception as e:
            logger.error(f'Error deleting all plots from Nexus API: {e}')

    @staticmethod
    def delete_all_rewards(base_url):
        try:
            logger.info('Deleting all rewards')
            local_url = f"{base_url}/delete/rewards/all"
            response = requests.post(local_url)
            logger.info(response.json())
        except Exception as e:
            logger.error(f'Error deleting all rewards from Nexus API: {e}')

    @staticmethod
    def delete_all_events(base_url):
        try:
            logger.info('Deleting all events')
            local_url = f"{base_url}/delete/events/all"
            response = requests.post(local_url)
            logger.info(response.json())
        except Exception as e:
            logger.error(f'Error deleting all events from Nexus API: {e}')