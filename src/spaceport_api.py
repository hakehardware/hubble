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
            logger.error(f'S-API: Error inserting Node via Nexus API: {e}')

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
            elif response.status_code == 200:
                logger.info(f"S-API: {json_data.get('message')}")
            else:
                logger.warn(f"S-API: Error inserting Node Event {json_data}")

        except Exception as e:
            logger.error(f'S-API: Error inserting Node Event via Nexus API: {e}')

    def insert_farmer_event(base_url, event):
        try:
            local_url = f'{base_url}/farmerEvents'

            data = {
                'eventDatetime': event.get('Datetime'),
                'farmerName': event.get('Farmer Name'),
                'type': event.get('Event Type'),
                'data': json.dumps(event.get('Data'))
            }

            response = requests.post(local_url, json=data)
            json_data = response.json()

            if response.status_code == 201:
                logger.info("S-API: Farmer Event Inserted")
                return True
            
            elif response.status_code == 200:
                logger.info(f"S-API: {json_data.get('message')}")
                    
            else:
                logger.warn(f"S-API: Error inserting Farmer Event {json_data}")

        except Exception as e:
            logger.error(f'S-API: Error inserting Farmer Event via Nexus API: {e}')


    # FARMER
    @staticmethod
    def get_farmers(base_url):
        try:
            local_url = f'{base_url}/farmers'
            response = requests.get(local_url)
            json_data = response.json()

            if response.status_code < 300:
                return json_data
            else:
                logger.error(f"S-API: Error getting Farmers {json_data.get('error')}")
                return None

        except Exception as e:
            logger.error(f'S-API: Error getting Farmers via Nexus API')

    @staticmethod
    def insert_farmer(base_url, data):
        try:
            local_url = f'{base_url}/farmers'

            logger.info(data)
            response = requests.post(local_url, json=data)
            json_data = response.json()

            if response.status_code == 201:
                logger.info("S-API: Farmer Inserted")
            else:
                logger.error(f"S-API: Error inserting Farmer {json_data}")

        except Exception as e:
            logger.error(f'S-API: Error inserting Farmer via Nexus API: {e}')

    @staticmethod
    def update_farmer(base_url, data):
        try:
            local_url = f"{base_url}/farmers/{data.get('name')}"

            response = requests.put(local_url, json=data)
            json_data = response.json()

            if response.status_code == 200:
                logger.info("S-API: Farmer Updated")
            else:
                logger.error(f"S-API: Error updating Farmer {json_data.get('error')}")

        except Exception as e:
            logger.error(f'S-API: Error updating Farmer')