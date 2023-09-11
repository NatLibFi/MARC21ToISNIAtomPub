#import pymarc
from pymarc import Record, Field
from pymarc import XmlHandler as OAIHandler
from lxml import etree as ET
import xml.sax
from xml.sax import make_parser
from xml.sax.handler import feature_namespaces
import unicodedata
import io

NAMESPACES = {'oai': 'http://www.openarchives.org/OAI/2.0/'}

def startElementNS(self, name, qname, attrs):
    """monkey patched pymarc function for OAI-PMH response"""
    element = name[1]
    self._text = []

    if element in ["oai_marc", "record"]:
        self._record = Record(force_utf8=True)
    elif element == "fixfield":
        tag = attrs.getValue((None, u"id"))
        self._field = Field(tag)
    elif element == "varfield":
        tag = attrs.getValue((None, u"id"))
        ind1 = attrs.get((None, u"i1"), u" ")
        ind2 = attrs.get((None, u"i2"), u" ")
        self._field = Field(tag, [ind1, ind2])
    elif element == "controlfield":
        tag = attrs.getValue((None, u"tag"))
        self._field = Field(tag)
    elif element == "datafield":
        tag = attrs.getValue((None, u"tag"))
        ind1 = attrs.get((None, u"ind1"), u" ")
        ind2 = attrs.get((None, u"ind2"), u" ")
        self._field = Field(tag, [ind1, ind2])
    elif element == "subfield":
        # regular MARCXML:
        if name[0] == 'http://www.loc.gov/MARC21/slim':
            self._subfield_code = attrs[(None, "code")]
        else:
            self._subfield_code = attrs[(None, "label")]

def endElementNS(self, name, qname):
    """monkey patched pymarc function for OAI-PMH response"""
    element = name[1]
    if self.normalize_form is not None:
        text = unicodedata.normalize(self.normalize_form, u"".join(self._text))
    else:
        text = u"".join(self._text)
    if element in ["oai_marc", "record"]:
        self.process_record(self._record)
        self._record = None
    elif element == "leader":
        self._record.leader = text.replace(' ', '^')
    elif element in ["fixfield", "controlfield"]:
        if self._field.tag == "LDR":
            self._record.leader = text
        else:
            if self._field.tag == "008":
                text = text.replace(' ', '^')
            self._field.data = text
            self._record.add_field(self._field)
        self._field = None
    elif element in ["varfield", "datafield"]:
        self._record.add_field(self._field)
        self._field = None
    elif element == "subfield":
        self._field.add_subfield(self._subfield_code, text)
        self._subfield_code = None

    self._text = []

def get_identifiers(response, parameters):
    root = ET.fromstring(bytes(response, encoding='utf-8'))
    identifiers = set()
    verb = parameters['verb']
    for identifier in root.findall('oai:' + verb + '/oai:record/oai:header/oai:identifier', NAMESPACES):
        identifiers.add(identifier.text.split('/')[-1])
    return identifiers

def get_resumption_token(response, parameters):
    verb = parameters['verb']
    root = ET.fromstring(bytes(response, encoding='utf-8'))
    for token in root.findall('oai:' + verb + '/oai:resumptionToken', NAMESPACES):
        return token.text

def get_records(response, parameters=None):
    root = ET.fromstring(bytes(response, encoding='utf-8'))
    # these pymarc functions are overridden by parse_oai_response 
    OAIHandler.startElementNS = startElementNS
    OAIHandler.endElementNS = endElementNS
    marc_records = []
    # path for X query:
    if not parameters:
        path = 'record'
    else:
        verb = parameters['verb']
        #path = 'oai:' + verb + '/oai:record/oai:metadata/oai:record'
        path = 'oai:' + verb + '/oai:record/oai:metadata'
    for records in root.findall(path, NAMESPACES):
        string_et = ET.tostring(records, encoding='utf-8', method='xml')
        string_xml = string_et.decode("utf-8") 
        f = io.StringIO(string_xml)
        handler = OAIHandler()
        parser = make_parser()
        parser.setContentHandler(handler)
        parser.setFeature(feature_namespaces, 1)
        parser.parse(f)
        marc_records.extend(handler.records)

    return marc_records