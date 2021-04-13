import io
from lxml import etree as ET
from pymarc import XmlHandler, Field, Record
import xml.sax
from xml.sax import make_parser
from xml.sax.handler import ContentHandler, feature_namespaces

NAMESPACES = {'zs': 'http://docs.oasis-open.org/ns/search-ws/sruResponse',
              'srw': 'http://www.loc.gov/zing/srw/'}

def startElementNS(self, name, qname, attrs):
    """Start element NS."""
    if self._strict and name[0] != MARC_XML_NS:
        return

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
    """End element NS."""
    if self._strict and name[0] != MARC_XML_NS:
        return

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
        self._field.subfields.append(self._subfield_code)
        self._field.subfields.append(text)
        self._subfield_code = None

    self._text = []

def get_isni_identifier(response):
    isni_path = 'responseRecord/ISNIAssigned/isniUnformatted'
    ppn_path = 'responseRecord/noISNI/PPN'
    root = ET.fromstring(bytes(response, encoding='utf-8'))
    for records in root.findall('srw:records', NAMESPACES):
        for record in records.findall('srw:record', NAMESPACES):
            for record_data in record.findall('srw:recordData', NAMESPACES):
                for isni in record_data.findall(isni_path):
                    return isni.text
                for ppn in record_data.findall(ppn_path):
                    return ppn.text
    
def get_number_of_records(response):
    tree = ET.ElementTree(ET.fromstring(response))
    root = tree.getroot()
    for number in root.findall('zs:numberOfRecords', NAMESPACES):
        return int(number.text)

def get_records(response):
    marc_records = []
    tree = ET.ElementTree(ET.fromstring(response))
    root = tree.getroot()
    # these functions are overridden by parse_oai_response 
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

    """
    handler = marcxml.XmlHandler()
    parser = xml.sax.make_parser()
    print(type(parser))
    parser.setContentHandler(handler)
    parser.setFeature(feature_namespaces, 1)
    parser.parse(url)
    print("handler.all_properties")
    print(parser.all_properties)
    print(CharacterDataHandler)
    return handler.records

    
    tree = ET.ElementTree(ET.fromstring(response))
    root = tree.getroot()
    marc_records = []
    for records in root.findall('zs:records', namespaces):
        for record in records.findall('zs:record', namespaces):
            for record_data in record.findall('zs:recordData', namespaces):
                handler = marcxml.XmlHandler()
                for marc_record in record_data:
                #data = bytes(record_data)
                #records = parse_xml_to_array(bytes(record_data))
                    
                    stringi = ET.tostring(marc_record)
                    lines = "".join(map(chr, stringi)).splitlines()
                    handler.process_record(stringi)
                    print(handler.records)
                    #handle = open('testi.xml', "wb")
                    #handle.write(ET.tostring(marc_record, encoding='UTF-8', pretty_print=True, xml_declaration=True))
                    #marc_records = parse_xml_to_array(open('testi.xml'))
                    stringi = ET.tostring(marc_record)
                    print("type")
                    print(type(stringi))
                    lines = "".join(map(chr, stringi)).splitlines()
                    lines = "".join(map(chr, stringi))
                    
                    #parser = xml.sax.make_parser()
                    #parser.setContentHandler(handler)
                    #parser.setFeature(feature_namespaces, 1)

                    #parser.parse(stringi)
                    #parser.parseString(ET.tostring(marc_record), handler)
                    xml.sax.parseString(stringi, handler)
                    
                    print(type(marc_record))
                    xml.sax.parseString(lines, handler)
                    #xml.sax.parse('testi.xml', handler)
                    print("handler")
                    print(handler.records)

    """