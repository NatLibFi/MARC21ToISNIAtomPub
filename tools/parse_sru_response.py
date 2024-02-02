import io
import json
from lxml import objectify, etree as ET
from pymarc import XmlHandler, Field, Record
import xml.sax
from xml.sax import make_parser
from xml.sax.handler import ContentHandler, feature_namespaces
import unicodedata

NAMESPACES = {'zs': 'http://docs.oasis-open.org/ns/search-ws/sruResponse'}

def startElementNS(self, name, qname, attrs):
    """monkey patched pymarc function for OAI-PMH response"""

    element = name[1]
    self._text = []

    if element == "record":
        self._record = Record()
    elif element == "controlfield":
        tag = attrs.getValue((None, u"tag"))
        self._field = Field(tag)
    elif element == "datafield":
        tag = attrs.getValue((None, u"tag"))
        ind1 = attrs.get((None, u"ind1"), u" ")
        ind2 = attrs.get((None, u"ind2"), u" ")
        self._field = Field(tag, [ind1, ind2])
    elif element == "subfield":
        self._subfield_code = attrs[(None, "code")]

def endElementNS(self, name, qname):
    """monkey patched pymarc function for OAI-PMH response"""

    element = name[1]
    if self.normalize_form is not None:
        text = unicodedata.normalize(self.normalize_form, u"".join(self._text))
    else:
        text = u"".join(self._text)

    if element == "record":
        self.process_record(self._record)
        self._record = None
    elif element == "leader":
        self._record.leader = text
    elif element == "controlfield":
        self._field.data = text
        self._record.add_field(self._field)
        self._field = None
    elif element == "datafield":
        self._record.add_field(self._field)
        self._field = None
    elif element == "subfield":
        self._field.add_subfield(self._subfield_code, text)
        self._subfield_code = None

    self._text = []

def get_number_of_records(response):
    tree = ET.ElementTree(ET.fromstring(response))
    root = tree.getroot()
    for number in root.findall('zs:numberOfRecords', NAMESPACES):
        return int(number.text)

def get_records(response):
    marc_records = []
    tree = ET.ElementTree(ET.fromstring(response))
    root = tree.getroot()
    # these pymarc functions are overridden by parse_oai_response 
    XmlHandler.startElementNS = startElementNS
    XmlHandler.endElementNS = endElementNS
    for records in root.findall('zs:records', NAMESPACES):
        for record in records.findall('zs:record', NAMESPACES):
            for record_data in record.findall('zs:recordData', NAMESPACES):
                string_et = ET.tostring(record_data, encoding='utf-8', method='xml')
                string_xml = string_et.decode("utf-8") 
                f = io.StringIO(string_xml)
                handler = XmlHandler()
                parser = xml.sax.make_parser()
                parser.setContentHandler(handler)
                parser.setFeature(feature_namespaces, 1)
                parser.parse(f)
                marc_records.extend(handler.records)
    return marc_records