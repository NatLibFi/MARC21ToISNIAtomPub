import argparse
import logging
import configparser
from tools.api_query import APIQuery

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", required=True,
        help="File path for configuration file structured for Python ConfigParser")
    parser.add_argument("-u", "--username",
        help="Username for ISNI SRU API")
    parser.add_argument("-p", "--password",
        help="Password for ISNI SRU API")
    parser.add_argument("-s", "--config_section",
        help="Name of the section in configuration file", required=True)
    parser.add_argument("-c", "--config_file_path",
        help="File path for configuration file structured for Python ConfigParser", required=True)
    parser.add_argument("-f", "--query_file",
        help="File path for saving and loading query results", required=True)
    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.config_file_path)
    config_section = config[args.config_section]
    query = APIQuery(config_section, args.username, args.password)
    query.get_isni_query_data(args.query, args.query_file)