#import pymarc
from pymarc import Record, Field
from pymarc import XmlHandler as OAIHandler
from lxml import etree as ET
import xml.sax
from xml.sax import make_parser
from xml.sax.handler import ContentHandler, feature_namespaces
import unicodedata
import io

NAMESPACES = {'oai': 'http://www.openarchives.org/OAI/2.0/'}
              
def startElementNS(self, name, qname, attrs):
    """Start element NS."""

    element = name[1]
    self._text = []

    if element == "oai_marc":
        self._record = Record()
    elif element == "fixfield":
        tag = attrs.getValue((None, u"id"))
        self._field = Field(tag)
    elif element == "varfield":
        tag = attrs.getValue((None, u"id"))
        ind1 = attrs.get((None, u"i1"), u" ")
        ind2 = attrs.get((None, u"i2"), u" ")
        self._field = Field(tag, [ind1, ind2])
    elif element == "subfield":
        self._subfield_code = attrs[(None, "label")]

def endElementNS(self, name, qname):
    """End element NS."""

    element = name[1]
    if self.normalize_form is not None:
        text = unicodedata.normalize(self.normalize_form, u"".join(self._text))
    else:
        text = u"".join(self._text)

    if element == "oai_marc":
        self.process_record(self._record)
        self._record = None
    elif element == "fixfield":
        self._field.data = text
        self._record.add_field(self._field)
        self._field = None
    elif element == "varfield":
        self._record.add_field(self._field)
        self._field = None
    elif element == "subfield":
        self._field.subfields.append(self._subfield_code)
        self._field.subfields.append(text)
        self._subfield_code = None

    self._text = []

def get_identifiers(response):
    root = ET.fromstring(bytes(response, encoding='utf-8'))
    identifiers = []
    for list in root.findall('oai:ListIdentifiers', NAMESPACES):
        for record in list.findall('oai:record', NAMESPACES):
            for header in record.findall('oai:header', NAMESPACES):
                for identifier in header.findall('oai:identifier', NAMESPACES):
                    identifiers.append(identifier.text.split('/')[-1])
    return identifiers
     
def get_records(response):
    root = ET.fromstring(bytes(response, encoding='utf-8'))
    #pymarc.marcxml.XmlHandler.startElementNS = startElementNS
    #pymarc.marcxml.XmlHandler.endElementNS = endElementNS
    OAIHandler.startElementNS = startElementNS
    OAIHandler.endElementNS = endElementNS
    marc_records = []
    for records in root.findall('record'):
        string_et = ET.tostring(records, encoding='utf-8', method='xml')
        string_xml = string_et.decode("utf-8") 
        f = io.StringIO(string_xml)
        #handler = pymarc.marcxml.XmlHandler()
        handler = OAIHandler()
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)
        parser.setFeature(feature_namespaces, 1)
        parser.parse(f)
        marc_records.extend(handler.records)
    return marc_records