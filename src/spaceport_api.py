import requests
from src.logger import logger
import uuid
import random
import json

class SpaceportAPI:
    # NODE
    @staticmethod
    def get_nodes(base_url):
        try:
            local_url = f'{base_url}/nodes'
            response = requests.get(local_url)
            json_data = response.json()

            if response.status_code < 300:
                return json_data
            else:
                logger.error(f"S-API: Error getting nodes {json_data.get('error')}")
                return None

        except Exception as e:
            logger.error(f'S-API: Error getting Nodes via Nexus API')

    @staticmethod
    def insert_node(base_url, data):
        try:
            local_url = f'{base_url}/nodes'

            response = requests.post(local_url, json=data)
            json_data = response.json()

            if response.status_code == 201:
                logger.info("S-API: Node Inserted")
            else:
                logger.error(f"S-API: Error inserting Node {json_data.get('error')}")

        except Exception as e:
            logger.error(f'S-API: Error inserting Node via Nexus API')

    @staticmethod
    def update_node(base_url, data):
        try:
            local_url = f"{base_url}/nodes/{data.get('name')}"

            response = requests.put(local_url, json=data)
            json_data = response.json()

            if response.status_code == 200:
                logger.info("S-API: Node Updated")
            else:
                logger.error(f"S-API: Error updating Node {json_data.get('error')}")

        except Exception as e:
            logger.error(f'S-API: Error updating Node')

    # NODE EVENTS
    @staticmethod
    def insert_consensus(base_url, event):
        try:
            local_url = f'{base_url}/consensus'

            data = {
                'consensusDatetime': event.get('Datetime'),
                'nodeName': event.get('Node Name'),
                'type': event.get('Event Type'),
                'peers': event.get('Data').get('Peers', 0),
                'best': event.get('Data').get('Best', 0),
                'target': event.get('Data').get('Target', 0),
                'finalized': event.get('Data').get('Finalized', 0),
                'bps': event.get('Data').get('BPS', 0),
                'downSpeed': event.get('Data').get('Down Speed', 0),
                'upSpeed': event.get('Data').get('Up Speed', 0)
            }

            response = requests.post(local_url, json=data)
            json_data = response.json()

            if response.status_code == 201:
                logger.info("S-API: Node Consensus Inserted")
            else:
                logger.info(json_data)

        except Exception as e:
            logger.error(f'S-API: Error inserting Consensus via S-API: {e}')
        
    @staticmethod
    def insert_claim(base_url, event):
        try:
            local_url = f'{base_url}/claims'

            data = {
                'claimDatetime': event.get('Datetime'),
                'nodeName': event.get('Node Name'),
                'slot': event.get('Data').get('Slot'),
                'type': event.get('Data').get('Type')
            }

            response = requests.post(local_url, json=data)
            json_data = response.json()

            if response.status_code == 201:
                logger.info("S-API: Claim Inserted")
            else:
                logger.info(json_data)


        except Exception as e:
            logger.error(f'S-API: Error inserting claim via S-API')
            return False
        

    # RAW EVENTS
    @staticmethod
    def insert_node_event(base_url, event):
        try:
            local_url = f'{base_url}/nodeEvents'

            data = {
                'eventDatetime': event.get('Datetime'),
                'nodeName': event.get('Node Name'),
                'type': event.get('Event Type'),
                'data': json.dumps(event.get('Data'))
            }

            response = requests.post(local_url, json=data)
            json_data = response.json()

            if response.status_code == 201:
                logger.info("S-API: Node Event Inserted")
                return True
                    
            else:
                logger.error(f"S-API: Error inserting Node Event {json_data}")

        except Exception as e:
            logger.error(f'S-API: Error inserting Node Event via Nexus API: {e}')










    # FARMER
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
    def insert_farmer_event(base_url, event):
        try:
            local_url = f'{base_url}/insert/farmer_event'
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
            logger.error(f'NEXUS: Error inserting farmer event via Nexus API')
            return False

    # FARM
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

    # DATA
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



    # DELETE
    @staticmethod
    def delete_all_plots(base_url):
        try:
            logger.info('Deleting all plots')
            local_url = f"{base_url}/delete/plots/all"
            response = requests.post(local_url)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"API: {message}")
            else:
                logger.error(f"API: {message}")
        except Exception as e:
            logger.error(f'Error deleting all plots from Nexus API: {e}')

    @staticmethod
    def delete_all_rewards(base_url):
        try:
            logger.info('Deleting all rewards')
            local_url = f"{base_url}/delete/rewards/all"
            response = requests.post(local_url)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"API: {message}")
            else:
                logger.error(f"API: {message}")
        except Exception as e:
            logger.error(f'Error deleting all rewards from Nexus API: {e}')

    @staticmethod
    def delete_all_farmer_events(base_url):
        try:
            logger.info('Deleting all events')
            local_url = f"{base_url}/delete/farmer_events/all"
            response = requests.post(local_url)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"API: {message}")
            else:
                logger.error(f"API: {message}")
        except Exception as e:
            logger.error(f'Error deleting all farmer events from Nexus API: {e}')

    @staticmethod
    def delete_all_node_events(base_url):
        try:
            logger.info('Deleting all events')
            local_url = f"{base_url}/delete/node_events/all"
            response = requests.post(local_url)
            json_data = response.json()

            success = json_data.get('Success')
            message = json_data.get('Message')

            if success:
                logger.info(f"API: {message}")
            else:
                logger.error(f"API: {message}")
        except Exception as e:
            logger.error(f'Error deleting all node events from Nexus API: {e}')