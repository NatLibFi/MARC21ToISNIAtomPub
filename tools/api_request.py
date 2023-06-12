import requests
import requests
import logging
import time
import json
import io
import os
import datetime
from pymarc import XMLWriter, parse_xml_to_array
from lxml import etree as ET
from sys import stdout

def successful_poll(response):
    """
    Checks that inserting POST requests is done
    :param response: Response from REST API
    """
    if hasattr(response, 'text'):
        response_text = json.loads(response.text)    
        state = response_text[0]['queueItemState']
        return state == 'DONE'
    return False

def get_request(id, config):
    """
    Gets MARC21 record via REST API
    :param id: Identifiers of MARC21 record
    :param config: Configuration data parsed by ConfigParser
    """
    auth = (os.environ['API_USERNAME'], os.environ['API_PWD'])
    headers = json.loads(config['AUT NAMES API'].get('accept_headers'))
    rest_url = config['AUT NAMES API'].get('baseurl')
    response = requests.get(rest_url + "/" + id, headers=headers, auth=auth)
    string_io = io.StringIO(response.text)
    record = parse_xml_to_array(string_io, strict=False)[0]

    return record

def send_isni_record(record, config):
    """
    Sends MARC21 record with new ISNI identifiers to REST API
    :param record: MARC21 record data
    :param config: Configuration data parsed by ConfigParser
    """
    section = config['AUT NAMES API']
    headers = json.loads(section.get('content_headers'))
    params = json.loads(section.get('parameters'))
    auth = (os.environ['API_USERNAME'], os.environ['API_PWD'])
    rest_url = section.get('baseurl')
    
    memory = io.BytesIO()
    writer = XMLWriter(memory)
    writer.write(record)
 
    writer.close(close_fh=False)
    memory.seek(0)
    tree = ET.parse(memory)
    xml = ET.tostring(tree, encoding='UTF-8')
    xml = xml.decode("utf-8")
    xml = xml.replace("</record>", "</record> ")
    xml = xml.encode("utf-8")
    
    response = requests.post(rest_url + '/bulk/', 
                             data=xml, headers=headers, auth=auth, params=params, timeout=20)
    response_text = json.loads(response.text)
    correlation_id = response_text['value']['correlationId']
    logging.info("correlation_id %s"%correlation_id)

    response = None
    retries = 0
    while not successful_poll(response) and retries < 10:
        response = requests.get(rest_url + '/bulk/?id='+correlation_id, headers=headers, auth=auth)
        time.sleep(3)
        retries += 1

    if not response:
        raise ValueError("polling unsuccesful")
    else:
        response_text = json.loads(response.text)[0]
        if 'correlationId' in response_text:
            logging.info(response_text['correlationId'])
            logging.info(datetime.datetime.now().replace(microsecond=0).isoformat())
        if 'handledIds' in response_text:
            logging.info('handledIds')
            logging.info(response_text['handledIds'])
            id_counter += len(response_text['handledIds'])
        if 'rejectedIds' in response_text:
            if response_text['rejectedIds']:
                logging.info('rejectedIds')
                logging.info(response_text['rejectedIds'])

    return response_text
