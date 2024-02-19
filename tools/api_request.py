import requests
import logging
import time
import json
import os
from pymarc import MARCReader, XMLWriter, record_to_xml
from lxml import etree as ET
import io
import sys

def _trim_undefined(record):
    """
    Remove undefined values from GET request responses
    """
    for field in record.get_fields("CAT"):
        for idx, sf in enumerate(field.subfields):
            if idx % 2 == 0 and sf == "b":
                if field.subfields[idx + 1] == "undefined":
                    field.subfields[idx + 1] = ""

def _successful_poll(response):
    if hasattr(response, 'text'):
        response_text = json.loads(response.text)
        state = response_text[0]['queueItemState']
        return state == 'DONE'
    return False

class APIRequest():
    def __init__(self, config_section):
        """
        class to make REST API requests
        : param config_section: a section of config file parsed by ConfigParser
        """
        try:
            self.accept_headers = {'Accept': 'application/marc'}
            self.content_headers = {'Content-type': 'application/xml'}
            self.params = json.loads(config_section.get('content_parameters'))
            self.rest_url = config_section.get('baseurl')
            self.time_out = int(config_section.get('timeout'))
            self.bulk_size = int(config_section.get('bulk_size'))
            self.bulk_wait = int(config_section.get('bulk_size'))
            self.poll_time = None
            self.correlation_id = None
            self.bulk_records = []
        except json.decoder.JSONDecodeError as e:
            logging.error("%s malformatted in config.ini"%config_section)
            logging.error(e)
            sys.exit(2)
        self.auth = (os.environ['API_USERNAME'], os.environ['API_PWD'])

    def marc21_to_marcxml_records(self, records):
        """
        Converts MARC21 records to MARCXML
        : param records: list of MARC21 formatted records
        """
        memory = io.BytesIO()
        writer = XMLWriter(memory)
        for record in records:
            writer.write(record)
        writer.close(close_fh=False)
        memory.seek(0)
        tree = ET.parse(memory)
        marcxml = ET.tostring(tree, encoding='UTF-8')
        marcxml = marcxml.decode("utf-8")
        marcxml = marcxml.replace("</record>", "</record> ")
        marcxml = marcxml.encode("utf-8")

        return marcxml

    def bulk_request(self, record=None, wait=False):
        """
        Check bulk request status or make bulk request if no bulk in queue
        or wait for all of records to be bulk requested
        :param record: add record to bulk requested records
        :param wait: wait for all remaining records to be bulk requested
        """
        updated_records = set()
        if record:
            self.bulk_records.append(record)
        if self.correlation_id:
            retries = 1
            if wait:
                retries = 10
            while retries:
                updated_records = self.poll_request()
                if updated_records:
                    return updated_records
                retries -= 1
                if retries > 0:
                    time.sleep(3)
            if wait and retries == 0:
                raise ValueError("Polling for correlation id %s exceeded retry limit"%self.correlation_id)
            return updated_records
        else:
            if len(self.bulk_records) >= self.bulk_size or wait:
                if len(self.bulk_records) >= self.bulk_size:
                    request_records = self.bulk_records[:self.bulk_size]
                    self.bulk_records = self.bulk_records[self.bulk_size:]
                elif wait:
                    request_records = self.bulk_records
                    self.bulk_records = []
                marcxml = self.marc21_to_marcxml_records(request_records)
                request_url = self.rest_url + '/bulk/'
                if self.poll_time:
                    interval = time.time() - self.poll_time
                    if interval < self.bulk_wait:
                        wait_time = self.bulk_wait - interval
                        time.sleep(wait_time)
                response = requests.post(request_url,
                                        data=marcxml,
                                        headers=self.content_headers,
                                        auth=self.auth,
                                        params=self.params,
                                        timeout=self.time_out)
                self.poll_time = time.time()
                response_text = json.loads(response.text)
                self.correlation_id = response_text['value']['correlationId']
                logging.info("Bulk correlation_id %s"%self.correlation_id)

        return updated_records

    def get_record(self, record_id):
        """
        API request record with id
        : param record_id: Identifier of MARC21 authority record
        """
        response = requests.get(self.rest_url + '/' + record_id, headers=self.accept_headers, auth=self.auth)
        reader = MARCReader(str.encode(response.text), to_unicode=True)
        record = next(reader)
        if record:
            _trim_undefined(record)
            return record
        else:
            logging.error("Requested record %s not found"%record_id)

    def post_request(self, record, id):
        """
        API request records with ISNI id
        : param records: list of records in MARC21 format
        """
        record = record_to_xml(record)
        response = requests.post(self.rest_url + id,
                                 data=record,
                                 headers=self.content_headers,
                                 auth=self.auth,
                                 timeout=self.time_out)
        if response.status_code != 200:
            raise Exception(str(response.status_code))

    def poll_request(self):
        """
        Poll bulk request response
        """
        if time.time() - self.poll_time > 3:
            response = requests.get(self.rest_url + '/bulk/?id=' + self.correlation_id, headers=self.content_headers , auth=self.auth)
            if _successful_poll(response):
                self.correlation_id = None
                response = json.loads(response.text)[0]
                updated_records = set()
                for record in response['records']:
                    if record['recordStatus'] == 'UPDATED':
                        updated_records.add(record['databaseId'])
                return updated_records
            self.poll_time = time.time()