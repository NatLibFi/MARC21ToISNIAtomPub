import logging
from pymarc import MARCReader, Record, Field, XMLWriter
import sys, os, pprint
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString


class MARC21ToISNIMARC:
    """
        A class to transform mrc binary files from MARC21 format to ISNIMARC format.
        Takes inputfilename and outputfilename as parameters for infile and outfile fields,
        which are used for reading from and writing to the mrc files.

        converter = MARC21ToISNIMARC("inputfile.mrc", "outputfile.mrc")

        Convert using the conv method

        converter.conv()

        :param inputfilename: input file location
        :param outputfilename: output file location
        :param skip: records with this field
        :param countrycode: ISO Alpha-2 countrycode
    """


    def __init__(self, inputfilename, countrycode, skip=None):
        self.skip = skip
        self.infile = inputfilename
        self.countrycode = countrycode

    def __str__(self):
        """
        In a string context MARC21ToISNIMARC will return basic info of the object itself
        :return str: String representation of the object
        """
        return "Converting file %s to %s, <%s object at %s, %s bytes>" % (self.infile, self.outfile, self.__class__.__name__,
                                                                          hex(id(self)), self.__sizeof__())

    def __sizeof__(self):
        return sys.getsizeof(MARC21ToISNIMARC)

    def convert2Bin(self, outfile):
        """
        This method opens the specified files (input and output), reads infile, converts it and writes it into the outfile
        :param outputfilename:
        """
        logging.info("Starting mrc to mrc conversion...")

        with open(self.infile, 'rb') as fh:
            reader = MARCReader(fh, force_utf8=True, to_unicode=True)
            out = open(outfile, 'wb')
            i = 0
            for record in reader:
                logging.info("Converting record.")
                sys.stdout.write('.')
                sys.stdout.flush()
                if i % 5 == 0:
                    print("\rConverting", end="")
                r = self.makeRecord(record)
                out.write(r.as_marc())
                i += 1
            out.close()
            print("\rConversion done.")

    def convert2XML(self, outfile):
        """
        Converts mrc bin format to isni xml marc format
        :param outputfilename:
        """
        logging.info("Starting mrc to xml conversion...")
        with open(self.infile, 'rb') as fh:
            reader = MARCReader(fh, force_utf8=True, to_unicode=True)
            out = XMLWriter(open(outfile, 'wb'))
            i = 0
            for record in reader:
                logging.info("Converting record.")
                sys.stdout.write(".")
                sys.stdout.flush()
                if i % 5 == 0:
                    print("\rConverting", end="")
                r = self.makeRecord(record)
                out.write(r)
                i += 1
            out.close()
            print("\rConversion done.")

    def convert2ISNIRequestXML(self, dirname, dirmax=20):
        """
        Converts MARC21 to ISNI XML Requests. Creates directory dirname, and creates XML Request files into the folder.
        dirmax default is 20, so it makes 20 request xml files per folder before creating a new one.
        :param dirname:
        :param dirmax:
        """
        if self.skip:
            print("Skipping records with %s field" % self.skip)
        logging.info("Starting mrc to isni request conversion...")
        #pp = pprint.PrettyPrinter(indent=2)
        with open(self.infile, 'rb') as fh:
            if not os.path.exists(dirname):
                os.mkdir(dirname)
            reader = MARCReader(fh, force_utf8=True, to_unicode=True)
            i = 1
            dirinc = 1
            for record in reader:
                if any(f.tag == self.skip for f in record.fields):
                    continue
                logging.info("Converting record.")
                r = self.makeIsniRequest(record)
                #pp.pprint(r)
                xml = dicttoxml(r, root=False, attr_type=False)
                xml = parseString(xml).toprettyxml()
                if i % dirmax == 0:
                    dirinc += 1
                subdir = dirname+"/"+str(dirinc)
                if not(os.path.exists(subdir)):
                    os.mkdir(subdir)
                xmlfile = open(subdir+"/request_"+str(i)+".xml", 'wb+')
                xmlfile.write(bytes(xml, 'UTF-8'))
                xmlfile.close()
                i += 1

            print("Conversion done")



    def makeRecord(self, record):
        """
        Converts the record the pymarcs MARCReader has read to ISNIMARC format.
        Takes a whole record as a parameter and returns the converted record.
        :param record: Old MARC21 Record
        :return newrecord: New ISNIMARC Record
        """
        newrecord = Record(force_utf8=True, to_unicode=True)
        for field in record.fields:
            if field.tag == '024':
                newrecord.add_field(Field(tag='035', indicators=['\\', '\\'], subfields=['a', record['024']['a']], marctype="isni"))

            elif field.tag == '110':
                newrecord.add_field(Field(tag='710', indicators=['2', '\\'], subfields=['a', record['110']['a']], marctype="isni"))
                if record['110']['b']:
                    newrecord.add_field(Field(tag='710', indicators=['2', '\\'], subfields=['b', record['110']['b']], marctype="isni"))

            elif field.tag == '111':
                newrecord.add_field(Field(tag='710', indicators=['\\', '\\'], subfields=['a', record['111']['a']], marctype="isni"))
                if record['111']['e']:
                    newrecord.add_field(Field(tag='710', indicators=['\\', '\\'], subfields=['a', record['111']['e']], marctype="isni"))

            elif field.tag == '046':
                if record['046']['s']:
                    newrecord.add_field(Field(tag='970', indicators=['\\', '\\'], subfields=['a', record['046']['s']], marctype="isni"))
                if record['046']['t']:
                    newrecord.add_field(Field(tag='970', indicators=['\\', '\\'], subfields=['b', record['046']['t']], marctype="isni"))
                if record['046']['q']:
                    newrecord.add_field(Field(tag='970', indicators=['\\', '\\'], subfields=['a', record['046']['q']], marctype="isni"))
                if record['046']['r']:
                    newrecord.add_field(Field(tag='970', indicators=['\\', '\\'], subfields=['b', record['046']['r']], marctype="isni"))
                if record['046']['f']:
                    newrecord.add_field(Field(tag='970', indicators=['\\', '\\'], subfields=['a', record['046']['f']], marctype="isni"))
                if record['046']['g']:
                    newrecord.add_field(Field(tag='970', indicators=['\\', '\\'], subfields=['b', record['046']['g']], marctype="isni"))

            elif field.tag == '411':
                if record['411']['a']:
                    newrecord.add_field(Field(tag='410', indicators=['\\', '\\'], subfields=['a', record['411']['a']], marctype="isni"))
                if record['411']['b']:
                    newrecord.add_field(Field(tag='410', indicators=['\\', '\\'], subfields=['a', record['411']['b']], marctype="isni"))

            elif field.tag == '370':
                if record['370']['e']:
                    newrecord.add_field(Field(tag='370', indicators=['\\', '\\'], subfields=['a', record['370']['e']], marctype="isni"))
                if record['370']['c']:
                    newrecord.add_field(Field(tag='922', indicators=['\\', '\\'], subfields=['a', record['370']['c']], marctype="isni"))
                if record['370']['e']:
                    newrecord.add_field(Field(tag='922', indicators=['\\', '\\'], subfields=['z', record['370']['e']], marctype="isni"))

            elif field.tag == '670':
                if record['670']['a']:
                    newrecord.add_field(Field(tag='670', indicators=['\\', '\\'], subfields=['a', record['670']['a']], marctype="isni"))
                if record['670']['b']:
                    newrecord.add_field(Field(tag='670', indicators=['\\', '\\'], subfields=['b', record['670']['b']], marctype="isni"))
                if record['670']['u']:
                    newrecord.add_field(Field(tag='670', indicators=['\\', '\\'], subfields=['u', record['670']['u']], marctype="isni"))

            elif field.tag == '377':
                if record['377']['a']:
                    newrecord.add_field(Field(tag='377', indicators=['\\', '\\'], subfields=['a', record['377']['a']], marctype="isni"))
                if record['377']['l']:
                    newrecord.add_field(Field(tag='670', indicators=['\\', '\\'], subfields=['a', record['670']['l']], marctype="isni"))

            elif field.tag == '510':
                if record['510']['a']:
                    newrecord.add_field(Field(tag='951', indicators=['\\', '\\'], subfields=['t', record['510']['a']], marctype="isni"))
                if record['510']['b']:
                    newrecord.add_field(Field(tag='951', indicators=['\\', '\\'], subfields=['t', record['510']['b']], marctype="isni"))

            elif field.tag == '020':
                newrecord.add_field(Field(tag='901', indicators=['\\', '\\'], subfields=['a', record['020']['a']], marctype="isni"))

            elif field.tag == '022':
                newrecord.add_field(Field(tag='902', indicators=['\\', '\\'], subfields=['a', record['022']['a']], marctype="isni"))

            elif field.tag == '024':
                newrecord.add_field(Field(tag='904', indicators=['\\', '\\'], subfields=['a', record['024']['a']], marctype="isni"))

            elif field.tag == '245':
                if record['245']['a']:
                    newrecord.add_field(Field(tag='910', indicators=['\\', '\\'], subfields=['a', record['245']['a']], marctype="isni"))
                if record['245']['b']:
                    newrecord.add_field(Field(tag='910', indicators=['\\', '\\'], subfields=['b', record['245']['b']], marctype="isni"))

            elif field.tag == '260':
                if record['260']['c']:
                    newrecord.add_field(Field(tag='943', indicators=['\\', '\\'], subfields=['a', record['260']['c']], marctype="isni"))

            elif field.tag == '264':
                if record['264']['c']:
                    newrecord.add_field(Field(tag='943', indicators=['\\', '\\'], subfields=['a', record['264']['c']], marctype="isni"))

            elif field.tag == '336':
                newrecord.add_field(Field(tag='944', indicators=['\\', '\\'], subfields=['a', record['336']['a']], marctype="isni"))

            elif field.tag == '110':
                if record['110']['e']:
                    newrecord.add_field(Field(tag='941', indicators=['\\', '\\'], subfields=['a', record['110']['e']], marctype="isni"))

            elif field.tag == '710':
                if record['710']['e']:
                    newrecord.add_field(Field(tag='941', indicators=['\\', '\\'], subfields=['a', record['710']['e']], marctype="isni"))

            elif field.tag == '260':
                if record['260']['b']:
                    newrecord.add_field(Field(tag='921', indicators=['\\', '\\'], subfields=['a', record['260']['b']], marctype="isni"))

            elif field.tag == '264':
                if record['264']['b']:
                    newrecord.add_field(Field(tag='921', indicators=['\\', '\\'], subfields=['a', record['264']['b']], marctype="isni"))

            elif field.tag == '100':
                if record['100']['a']:
                    newrecord.add_field(Field(tag='700', indicators=['\\', '\\'], subfields=['a', record['100']['a']], marctype="isni"))
                if record['100']['b']:
                    newrecord.add_field(Field(tag='700', indicators=['\\', '\\'], subfields=['b', record['100']['b']], marctype="isni"))
                if record['100']['c']:
                    newrecord.add_field(Field(tag='700', indicators=['\\', '\\'], subfields=['c', record['100']['c']], marctype="isni"))


            else:
                newrecord.add_field(field)

        ##Country field is obligatory for ISNIMARC
        newrecord.add_field(Field(tag='922', indicators=['\\', '\\'], subfields=['a', self.countrycode], marctype="isni"))
        return newrecord

    def makeIsniRequest(self, record):
        requestdict = {"Request": {"requestID": {"dateTimeOfRequest": "", "requestTransactionId": ""}, "identityInformation": {"identity": {"resource": {"titleOfWork": {},
                                                                                                                                                         "identifier": {},
                                                                                                                                                         "creationClass": {},
                                                                                                                                                         "creationRole": {}}}}}}
        for field in record.fields:
            if field.tag == '024':
                requestdict["Request"]["identityInformation"]["requestorIdentifierOfIdentity"] = {"otheridentifierOfIdentity": {"identifier": record['024']['a']}}
            elif field.tag == '035':
                requestdict["Request"]["identityInformation"]["requestorIdentifierOfIdentity"] = {"identifier": record['035']['a']}
            elif field.tag == '110':
                requestdict["Request"]["identityInformation"]["organisationName"] = {"mainName": record['110']['a']}
                if record['110']['b']:
                    requestdict["Request"]["identityInformation"]["organisationName"] = {"subdivisionName": record['110']['b']}
                if record['110']['e']:
                    requestdict["Request"]["identityInformation"]["identity"]["resource"]["creationRole"] = record['110']['e']
            elif field.tag == '368':
                requestdict["Request"]["identityInformation"]["organisationType"] = record['368']['a']
            elif field.tag == '046':
                if record['046']['s']:
                    requestdict["Request"]["identityInformation"]["usageDateFrom"] = record['046']['s']
                if record['046']['t']:
                    requestdict["Request"]["identityInformation"]["usageDateTo"] = record['046']['t']
                if record['046']['q']:
                    requestdict["Request"]["identityInformation"]["usageDateFrom"] = record['046']['q']
                if record['046']['r']:
                    requestdict["Request"]["identityInformation"]["usageDateTo"] = record['046']['r']
            elif field.tag == '410':
                requestdict["Request"]["identityInformation"]["organisationName"]["organisationNameVariant"] = record['410']['a']
            elif field.tag == '411':
                requestdict["Request"]["identityInformation"]["organisationName"]["organisationNameVariant"] = record['411']['a']
            elif field.tag == '370':
                if record['370']['e']:
                    requestdict["Request"]["identityInformation"]["location"] = {"countryCode": record['370']['e']}
            elif field.tag == '670':
                requestdict["Request"]["externalInformation"] = {"source": record['670']['a']}
                if record['670']['b']:
                    requestdict["Request"]["externalInformation"] = {"information": record['670']['b']}
                if record['670']['u']:
                    requestdict["Request"]["externalInformation"] = {"URI": record['670']['u']}
            elif field.tag == '377':
                requestdict["Request"]["identityInformation"]["languageOfIdentity"] = record['377']['a']
            elif field.tag == '020':
                requestdict["Request"]["identityInformation"]["identity"]["resource"]["identifier"] = {"identifierType": "ISBN", "identifierValue": record['020']['a']}
            elif field.tag == '022':
                requestdict["Request"]["identityInformation"]["identity"]["resource"]["identifier"] = {"identifierType": "ISSN", "identifierValue": record['022']['a']}
            elif field.tag == '024':
                requestdict["Request"]["identityInformation"]["identity"]["resource"]["identifier"] = {"identifierType": "Other", "identifierValue": record['024']['a']}
            elif field.tag == '245':
                requestdict["Request"]["identityInformation"]["identity"]["titleOfWork"] = {"title": record['245']['a']}
                if record['245']['b']:
                    requestdict["Request"]["identityInformation"]["identity"]["resource"]["titleOfWork"]["subtitle"] = record['245']['b']
            elif field.tag == '260':
                if record['260']['b']:
                    requestdict["Request"]["identityInformation"]["identity"]["resource"]["titleOfWork"]["publisher"] = record['260']['b']
                if record['260']['c']:
                    requestdict["Request"]["identityInformation"]["identity"]["resource"]["titleOfWork"]["date"] = record['245']['c']
            elif field.tag == '336':
                requestdict["Request"]["identityInformation"]["identity"]["resource"]["creationClass"] = record['336']['a']

        return requestdict