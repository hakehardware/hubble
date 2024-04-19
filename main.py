import argparse
import sys

from src.logger import logger

from src.helpers import Helpers
from src.hubble import Hubble

def check_config(config) -> bool:
    if 'farmer_name' not in config:
        logger.error(f'Farmer Name is required and missing from the config. Please see README.')
        return False
    
    return True

def main():

    # get arguments
    parser = argparse.ArgumentParser(description='Load and print YAML configuration.')
    parser.add_argument('config_file', metavar='config_file.yml', type=str,
                        help='path to the YAML configuration file')
    args = parser.parse_args()

    # parse config
    config = Helpers.read_yaml_file(args.config_file)

    # check if a config file was able to be loaded, if not throw error and exit
    if not config:
        logger.error(f'Error loading config from {args.config_file}. Are you sure you put in the right location?')
        sys.exit(1)
    
    # confirm that all required fields are present in the config
    valid_config = check_config(config)
    if not valid_config:
        sys.exit(1)
    
    # config looks good, proceed
    logger.info(f'Configuration loaded successfully: {config}')

    # run hubble
    hubble = Hubble(config)
    hubble.run()

if __name__ == "__main__":
    main()