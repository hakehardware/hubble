import src.constants as constants
import datetime
import re

from typing import Dict
from src.helpers import Helpers

class LogParser:
    def get_log_event(name, timestamp, level, data) -> Dict:
        event = None

        parsed_timestamp = timestamp.split('.')[0] + '.' + timestamp.split('.')[1][0:3]
        age = Helpers.check_age_of_timestamp(parsed_timestamp)

        if constants.KEY_EVENTS[0] in data:
            pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'        
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Plotting Sector',
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Level': level,
                    'Age': age,
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Percentage Complete': match.group(2),
                        'Current Sector': match.group(3),
                        'Replot': 0
                    }
                }
        elif constants.KEY_EVENTS[1] in data:
            pattern = r'Piece cache sync (\d+\.\d+)% complete'
            match = re.search(pattern, data)

            if match:
                event = {
                    'Event Type': 'Piece Cache Sync',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Percentage Complete': float(match.group(1)),
                        'Farmer Name': name
                    }
                }
        elif constants.KEY_EVENTS[2] in data:
            event = {
                    'Event Type': 'Plotting Paused',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farmer Name': name
                    }
            }
        elif constants.KEY_EVENTS[3] in data:
            event = {
                'Event Type': 'Plotting Resumed',
                'Level': level,
                'Age': age,
                'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farmer Name': name
                }
            }
        elif constants.KEY_EVENTS[4] in data:
            event = {
                'Event Type': 'Finished Piece Cache Sync',
                'Level': level,
                'Age': age,
                'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farmer Name': name
                }
            }
        elif constants.KEY_EVENTS[5] in data:
            pattern = r'farm_index=(\d+).*hash\s(0x[0-9a-fA-F]+)'
            match = re.search(pattern, data)

            event = {
                'Event Type': 'Reward',
                'Level': level,
                'Age': age,
                'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farm Index': match.group(1),
                    'Hash': match.group(2)
                }
            }
        elif constants.KEY_EVENTS[6] in data:
            pattern = r"Single disk farm (\d+):"

            # Use re.search() to find the match
            match = re.search(pattern, data)

            event = {
                'Event Type': 'New Farm Identified',
                'Level': level,
                'Age': age,
                'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farm Index': int(match.group(1))
                }
            }
        elif constants.KEY_EVENTS[7] in data:
            event = {
                'Event Type': 'Synchronizing Piece Cache',
                'Level': level,
                'Age': age,
                'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farmer Name': name
                }
            }
        elif constants.KEY_EVENTS[8] in data:
            pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'        
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Replotting Sector',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Percentage Complete': float(match.group(2)),
                        'Current Sector': int(match.group(3)),
                        'Replot': 1
                    }
                }
        elif constants.KEY_EVENTS[9] in data:
            pattern = r"farm_index=(\d+)"
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Replotting Complete',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                    }
                }
        elif constants.KEY_EVENTS[10] in data:
            pattern = r"farm_index=(\d+)"
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Failed to Send Solution',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Hash': None
                    }
                }
        elif constants.KEY_EVENTS[11] in data:
            pattern = r'starting (\d+) workers'
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Starting Workers',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Number of Workers': int(match.group(1)),
                        'Farmer Name': name
                    }
                }
        elif constants.KEY_EVENTS[12] in data:
            pattern = r'farm_index=(\d+).*ID:\s+([A-Z0-9]+)'
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Farm ID',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Farm ID': match.group(2),
                    }
                }
        elif constants.KEY_EVENTS[13] in data:
            pattern = r'farm_index=(\d+).*Public key:\s+(0x[a-fA-F0-9]+)'
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Farm Public Key',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Farm Public Key': match.group(2),
                    }
                }
        elif constants.KEY_EVENTS[14] in data:
            pattern = r'farm_index=(\d+).*Allocated space:\s+([\d.]+)\s+(GiB|TiB|GB|TB)\s+\(([\d.]+)\s+(GiB|TiB|GB|TB)\)'
            match = re.search(pattern, data)

            if match:
                if match.group(3) == 'TiB':
                    allocated_gib = float(match.group(2)) * 1024
                else:
                    allocated_gib = float(match.group(2))

                event = {
                    'Event Type': 'Farm Allocated Space',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Allocated Space Primary': allocated_gib                    
                    }
                }
        elif constants.KEY_EVENTS[15] in data:
            pattern = r'farm_index=(\d+).*Directory:\s+(.+)'
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Farm Directory',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Directory': match.group(2)
                    }
                }
        elif constants.KEY_EVENTS[16] in data:
            pattern = r"farm_index=(\d+)"
            match = re.search(pattern, data)
            if match:
                event = {
                    'Event Type': 'Plotting Complete',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                    }
                }
        elif constants.KEY_EVENTS[17] in data:
            pattern = r'Idle \((\d+) peers\), best: #(\d+).*finalized #(\d+).*⬇ (\d+(?:\.\d+)?)(?:kiB|MiB)?/s ⬆ (\d+(?:\.\d+)?)(?:kiB|MiB)?/s'
            match = re.search(pattern, data)

            if match:
                peers, best, finalized, down_speed, up_speed = match.groups()
                event = {
                    'Event Type': 'Idle Node',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Peers': peers,
                        'Best': best,
                        'Finalized': finalized,
                        'Down Speed': down_speed,
                        'Up Speed': up_speed
                    }
                }
        elif constants.KEY_EVENTS[18] in data:
            pattern = r'slot=(\d+)'
            match = re.search(pattern, data)

            if match:
                slot = int(match.group(1))
                event = {
                    'Event Type': 'Claimed Vote',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Slot': slot,
                    }
                }
        elif constants.KEY_EVENTS[19] in data:
            pattern = r'slot=(\d+)'
            match = re.search(pattern, data)
            
            if match:
                slot = int(match.group(1))
                event = {
                    'Event Type': 'Claimed Block',
                    'Level': level,
                    'Age': age,
                    'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Slot': slot,
                    }
                }
        else:
            event = {
                'Event Type': 'Unknown',
                'Level': level,
                'Age': age,
                'Datetime': datetime.datetime.strptime(parsed_timestamp, '%Y-%m-%dT%H:%M:%S.%f'),
                'data': {
                    'log': data
                }
            }

            
        return event
