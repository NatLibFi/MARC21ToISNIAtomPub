import logging
from pymarc import MARCReader, Record, Field, XMLWriter
import sys, os, pprint
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
import xml.etree.cElementTree as ET


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
        return "Converting file %s to %s, <%s object at %s, %s bytes>" % (
        self.infile, self.outfile, self.__class__.__name__,
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
        # pp = pprint.PrettyPrinter(indent=2)
        with open(self.infile, 'rb') as fh:
            if not os.path.exists(dirname):
                os.mkdir(dirname)
            reader = MARCReader(fh, force_utf8=True, to_unicode=True)
            i = 1
            dirinc = 0
            for record in reader:
                if any(f.tag == self.skip for f in record.fields):
                    continue
                logging.info("Converting record.")
                r = self.makeIsniRequest(record)
                xml = parseString(ET.tostring(r, "utf-8")).toprettyxml()
                if i % dirmax == 0:
                    dirinc += 1
                subdir = dirname + "/" + str(dirinc).zfill(3)
                if not (os.path.exists(subdir)):
                    os.mkdir(subdir)
                xmlfile = open(subdir + "/request_" + str(i).zfill(5) + ".xml", 'wb+')
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
                newrecord.add_field(
                    Field(tag='035', indicators=['\\', '\\'], subfields=['a', record['024']['a']], marctype="isni"))

            elif field.tag == '110':
                newrecord.add_field(
                    Field(tag='710', indicators=['2', '\\'], subfields=['a', record['110']['a']], marctype="isni"))
                if record['110']['b']:
                    newrecord.add_field(
                        Field(tag='710', indicators=['2', '\\'], subfields=['b', record['110']['b']], marctype="isni"))

            elif field.tag == '111':
                newrecord.add_field(
                    Field(tag='710', indicators=['\\', '\\'], subfields=['a', record['111']['a']], marctype="isni"))
                if record['111']['e']:
                    newrecord.add_field(
                        Field(tag='710', indicators=['\\', '\\'], subfields=['a', record['111']['e']], marctype="isni"))

            elif field.tag == '046':
                if record['046']['s']:
                    newrecord.add_field(
                        Field(tag='970', indicators=['\\', '\\'], subfields=['a', record['046']['s']], marctype="isni"))
                if record['046']['t']:
                    newrecord.add_field(
                        Field(tag='970', indicators=['\\', '\\'], subfields=['b', record['046']['t']], marctype="isni"))
                if record['046']['q']:
                    newrecord.add_field(
                        Field(tag='970', indicators=['\\', '\\'], subfields=['a', record['046']['q']], marctype="isni"))
                if record['046']['r']:
                    newrecord.add_field(
                        Field(tag='970', indicators=['\\', '\\'], subfields=['b', record['046']['r']], marctype="isni"))
                if record['046']['f']:
                    newrecord.add_field(
                        Field(tag='970', indicators=['\\', '\\'], subfields=['a', record['046']['f']], marctype="isni"))
                if record['046']['g']:
                    newrecord.add_field(
                        Field(tag='970', indicators=['\\', '\\'], subfields=['b', record['046']['g']], marctype="isni"))

            elif field.tag == '411':
                if record['411']['a']:
                    newrecord.add_field(
                        Field(tag='410', indicators=['\\', '\\'], subfields=['a', record['411']['a']], marctype="isni"))
                if record['411']['b']:
                    newrecord.add_field(
                        Field(tag='410', indicators=['\\', '\\'], subfields=['a', record['411']['b']], marctype="isni"))

            elif field.tag == '370':
                if record['370']['e']:
                    newrecord.add_field(
                        Field(tag='370', indicators=['\\', '\\'], subfields=['a', record['370']['e']], marctype="isni"))
                if record['370']['c']:
                    newrecord.add_field(
                        Field(tag='922', indicators=['\\', '\\'], subfields=['a', record['370']['c']], marctype="isni"))
                if record['370']['e']:
                    newrecord.add_field(
                        Field(tag='922', indicators=['\\', '\\'], subfields=['z', record['370']['e']], marctype="isni"))

            elif field.tag == '670':
                if record['670']['a']:
                    newrecord.add_field(
                        Field(tag='670', indicators=['\\', '\\'], subfields=['a', record['670']['a']], marctype="isni"))
                if record['670']['b']:
                    newrecord.add_field(
                        Field(tag='670', indicators=['\\', '\\'], subfields=['b', record['670']['b']], marctype="isni"))
                if record['670']['u']:
                    newrecord.add_field(
                        Field(tag='670', indicators=['\\', '\\'], subfields=['u', record['670']['u']], marctype="isni"))

            elif field.tag == '377':
                if record['377']['a']:
                    newrecord.add_field(
                        Field(tag='377', indicators=['\\', '\\'], subfields=['a', record['377']['a']], marctype="isni"))
                if record['377']['l']:
                    newrecord.add_field(
                        Field(tag='670', indicators=['\\', '\\'], subfields=['a', record['670']['l']], marctype="isni"))

            elif field.tag == '510':
                if record['510']['a']:
                    newrecord.add_field(
                        Field(tag='951', indicators=['\\', '\\'], subfields=['t', record['510']['a']], marctype="isni"))
                if record['510']['b']:
                    newrecord.add_field(
                        Field(tag='951', indicators=['\\', '\\'], subfields=['t', record['510']['b']], marctype="isni"))

            elif field.tag == '020':
                newrecord.add_field(
                    Field(tag='901', indicators=['\\', '\\'], subfields=['a', record['020']['a']], marctype="isni"))

            elif field.tag == '022':
                newrecord.add_field(
                    Field(tag='902', indicators=['\\', '\\'], subfields=['a', record['022']['a']], marctype="isni"))

            elif field.tag == '024':
                newrecord.add_field(
                    Field(tag='904', indicators=['\\', '\\'], subfields=['a', record['024']['a']], marctype="isni"))

            elif field.tag == '245':
                if record['245']['a']:
                    newrecord.add_field(
                        Field(tag='910', indicators=['\\', '\\'], subfields=['a', record['245']['a']], marctype="isni"))
                if record['245']['b']:
                    newrecord.add_field(
                        Field(tag='910', indicators=['\\', '\\'], subfields=['b', record['245']['b']], marctype="isni"))

            elif field.tag == '260':
                if record['260']['c']:
                    newrecord.add_field(
                        Field(tag='943', indicators=['\\', '\\'], subfields=['a', record['260']['c']], marctype="isni"))

            elif field.tag == '264':
                if record['264']['c']:
                    newrecord.add_field(
                        Field(tag='943', indicators=['\\', '\\'], subfields=['a', record['264']['c']], marctype="isni"))

            elif field.tag == '336':
                newrecord.add_field(
                    Field(tag='944', indicators=['\\', '\\'], subfields=['a', record['336']['a']], marctype="isni"))

            elif field.tag == '110':
                if record['110']['e']:
                    newrecord.add_field(
                        Field(tag='941', indicators=['\\', '\\'], subfields=['a', record['110']['e']], marctype="isni"))

            elif field.tag == '710':
                if record['710']['e']:
                    newrecord.add_field(
                        Field(tag='941', indicators=['\\', '\\'], subfields=['a', record['710']['e']], marctype="isni"))

            elif field.tag == '260':
                if record['260']['b']:
                    newrecord.add_field(
                        Field(tag='921', indicators=['\\', '\\'], subfields=['a', record['260']['b']], marctype="isni"))

            elif field.tag == '264':
                if record['264']['b']:
                    newrecord.add_field(
                        Field(tag='921', indicators=['\\', '\\'], subfields=['a', record['264']['b']], marctype="isni"))

            elif field.tag == '100':
                if record['100']['a']:
                    newrecord.add_field(
                        Field(tag='700', indicators=['\\', '\\'], subfields=['a', record['100']['a']], marctype="isni"))
                if record['100']['b']:
                    newrecord.add_field(
                        Field(tag='700', indicators=['\\', '\\'], subfields=['b', record['100']['b']], marctype="isni"))
                if record['100']['c']:
                    newrecord.add_field(
                        Field(tag='700', indicators=['\\', '\\'], subfields=['c', record['100']['c']], marctype="isni"))


            else:
                newrecord.add_field(field)

        ##Country field is obligatory for ISNIMARC
        newrecord.add_field(
            Field(tag='922', indicators=['\\', '\\'], subfields=['a', self.countrycode], marctype="isni"))
        return newrecord

    def makeIsniRequest(self, record):

        requestIdentifier = []
        personalName = []
        organisationTypes = []
        usageDateFrom = []
        usageDateTo = []
        locationCountryCode = []
        externalInformationSource = []
        externalInformationInfo = []
        externalInformationURI = []
        languageOfIdentity = []
        identifierISBN = []
        identifierISSN = []
        identifierOther = []
        titleOfWorkTitle = []
        titleOfWorkSubtitle = []
        titleOfWorkPublisher = []
        titleOfWorkDate = []
        creationClass = []
        otherIdentifier = []

        organisationVariants = []
        organisationMains = []

        relations = []

        for t in record.get_fields("410"):
            org = {"mainName": t['a']}
            if t['b']:
                org.update({"subdivisionName": t['b']})
            organisationVariants.append(org)

        for t in record.get_fields("411"):
            org = {"mainName": t['a']}
            if t['b']:
                org.update({"subdivisionName": t['b']})
            organisationVariants.append(org)

        for t in record.get_fields("510"):
            isRelated = {}
            if t['w'] is "a":
                isRelated = {"relationType": "supersedes", "identityType": "organisation", "noISNI": {"PPN": "", "organisationName": t['a']}}
                if t['b']:
                    isRelated.update({"noISNI": {"PPN": "", "organisationName": t['a'], "subDivisionName": t['b']}})
                relations.append(isRelated)
            elif t['w'] is "b":
                isRelated = {"relationType": "isSupersededBy", "identityType": "organisation", "noISNI": {"PPN": "", "organisationName": t['a']}}
                if t['b']:
                    isRelated.update({"noISNI": {"PPN": "", "organisationName": t['a'], "subDivisionName": t['b']}})
                relations.append(isRelated)

        for field in record.fields:
            if field.tag == '024':
                otherIdentifier.append(record['024']['a'])
            elif field.tag == '100':
                personalName.append(record['100']['a'])
            elif field.tag == '035':
                requestIdentifier.append(record['035']['a'])
            elif field.tag == '001':
                requestIdentifier.append("(FI-ASTERI-N)"+record['001'].data)
            elif field.tag == '110':
                #organisationMainNames.append(record['110']['a'])
                org = {"mainName": record['110']['a']}
                if record['110']['b']:
                    #organisationMainSubdivNames.append(record['110']['b'])
                    org.update({"subdivisionName": record['110']['b']})
                organisationMains.append(org)
            elif field.tag == '368':
                organisationTypes.append(record['368']['a'])
            elif field.tag == '046':
                if record['046']['s']:
                    usageDateFrom.append(record['046']['s'])
                if record['046']['t']:
                    usageDateTo.append(record['046']['t'])
                if record['046']['q']:
                    usageDateFrom.append(record['046']['q'])
                if record['046']['r']:
                    usageDateTo.append(record['046']['r'])
            elif field.tag == '370':
                if record['370']['e']:
                    locationCountryCode.append(record['370']['e'])
            elif field.tag == '670':
                externalInformationSource.append(record['670']['a'])
                if record['670']['b']:
                    externalInformationInfo.append(record['670']['b'])
                if record['670']['u']:
                    externalInformationURI.append(record['670']['u'])
            elif field.tag == '377':
                languageOfIdentity.append(record['377']['a'])
            elif field.tag == '020':
                identifierISBN.append(record['020']['a'])
            elif field.tag == '022':
                identifierISSN.append(record['022']['a'])
            elif field.tag == '024':
                identifierOther.append(record['024']['a'])
            elif field.tag == '245':
                titleOfWorkTitle.append(record['245']['a'])
                if record['245']['b']:
                    titleOfWorkSubtitle.append(record['245']['b'])
            elif field.tag == '260':
                if record['260']['b']:
                    titleOfWorkPublisher.append(record['260']['b'])
                if record['260']['c']:
                    titleOfWorkDate.append(record['245']['c'])
            elif field.tag == '336':
                creationClass.append(record['336']['a'])

        requestxml = ET.Element("Request")
        idinfoxml = ET.SubElement(requestxml, "identityInformation")
        identityxml = ET.SubElement(idinfoxml, "identity")
        requestoridxml = ET.SubElement(idinfoxml, "requestorIdentifierOfIdentity")
        organisationxml = ET.SubElement(identityxml, "organisation")

        if requestIdentifier:
            for c in list(set(requestIdentifier)):
                identifier = ET.SubElement(requestoridxml, "identifier")
                identifier.text = c

        if otherIdentifier:
            otheridentifierxml = ET.SubElement(idinfoxml, "otherIdentityOfIdentity")
            for c in list(set(otherIdentifier)):
                otherid = ET.SubElement(otheridentifierxml, "identifier")
                otherid.text = c

        if personalName:
            personorfictionxml = ET.SubElement(identityxml, "personOrFiction")
            personalNamexml = ET.SubElement(personorfictionxml, "personalName")
            namexml = ET.SubElement(personalNamexml, "name")
            namexml.text = list(set(personalName))[0]

        if organisationMains:
            orgnamexml = ET.SubElement(organisationxml, "organisationName")
            mainnamexml = ET.SubElement(orgnamexml, "mainName")

            for c in organisationMains:
                mainnamexml.text = c['mainName']
                if "subdivisionName" in c:
                    subdivnamexml = ET.SubElement(orgnamexml, "subdivisionName")
                    subdivnamexml.text = c['subdivisionName']

        if locationCountryCode:
            locationxml = ET.SubElement(organisationxml, "location")
            countrycodexml = ET.SubElement(locationxml, "countryCode")
            for c in list(set(locationCountryCode)):
                country, city = c.split(",")[1].strip().lower() if "," in c else c.lower(), c.split(",")[0].strip() if "," in c else None
                isocode = self.converttoiso(country)
                countrycodexml.text = isocode
                if city:
                    cityxml = ET.SubElement(locationxml, "city")
                    cityxml.text = city

        if relations:
            for c in relations:
                isrelatedxml = ET.SubElement(requestxml, "isRelated")
                idtypexml = ET.SubElement(isrelatedxml, "identityType")
                idtypexml.text = c['identityType']
                relationtypexml = ET.SubElement(isrelatedxml, "relationType")
                relationtypexml.text = c['relationType']
                noisnixml = ET.SubElement(isrelatedxml, "noISNI")
                ppnxml = ET.SubElement(noisnixml, "PPN")
                ppnxml.text = ""
                relorgnamexml = ET.SubElement(noisnixml, "organisationName")
                relorgmainxml = ET.SubElement(relorgnamexml, "mainName")
                relorgmainxml.text = c['noISNI']['organisationName']
                if c['noISNI'].get('subDivisionName') is not None:
                    subdivnamexml = ET.SubElement(relorgnamexml, "subdivisionName")
                    subdivnamexml.text = c['noISNI']['subDivisionName']

        #if organisationTypes:
         #   for c in organisationTypes:
          #      try:
           #         orgtypexml = ET.SubElement(organisationxml, "organisationType")
            #        orgtypexml.text = c
             #   except UnboundLocalError:
              #      organisationxml = ET.SubElement(identityxml, "organisation")
               #     orgtypexml = ET.SubElement(organisationxml, "organisationType")
                #    orgtypexml.text = c

        if organisationVariants:
            for c in organisationVariants:
                try:
                    orgnamevariantxml = ET.SubElement(organisationxml, "organisationNameVariant")
                    orgnamevariantnamexml = ET.SubElement(orgnamevariantxml, "mainName")
                    orgnamevariantnamexml.text = c['mainName']
                    if "subdivisionName" in c:
                        subdivnamexml = ET.SubElement(orgnamevariantxml, "subdivisionName")
                        subdivnamexml.text = c['subdivisionName']
                except UnboundLocalError:
                    organisationxml = ET.SubElement(identityxml, "organisation")
                    orgnamevariantxml = ET.SubElement(organisationxml, "organisationNameVariant")
                    orgnamevariantnamexml = ET.SubElement(orgnamevariantxml, "mainName")
                    orgnamevariantnamexml.text = c['mainName']
                    if "subdivisionName" in c:
                        subdivnamexml = ET.SubElement(orgnamevariantxml, "subdivisionName")
                        subdivnamexml.text = c['subdivisionName']

        return requestxml

    def converttoiso(self, country):
        convertdict = {"afganistan": "AF", "ahvenanmaa": "AX", "alankomaat": "NL", "albania": "AL", "algeria": "DZ", "andorra": "AD",
                       "angola": "AO", "arabiemiirikunnat": "AE", "argentiina": "AR", "armenia": "AM", "australia": "AU", "bahrain": "BH",
                       "bahama": "BS", "bangladesh": "BD", "barbados": "BB", "belgia": "BE", "belize": "BZ", "bolivia": "BO", "bosnia ja hertsegovina": "BA",
                       "brasilia": "BR", "bulgaria": "BG", "caymansaaret": "KY", "chile": "CL", "ecuador": "EC", "egypti": "EG", "espanja": "ES",
                       "etiopia": "ET", "etelä-afrikka": "ZA", "filippiinit": "PH", "georgia": "GE", "hongkong": "HK", "indonesia": "ID",
                       "intia": "IN", "irak": "IQ", "iran": "IR", "irlanti": "IE", "islanti": "IS", "israel": "IL", "italia": "IT",
                       "itävalta": "AT", "japani": "JP", "kazakstan": "KZ", "kenia": "KE", "kiina": "CN", "kolumbia": "CO",
                       "kreikka": "GR", "kroatia": "HR", "kuuba": "CU", "kuwait": "KW", "kypros": "CY", "latvia": "LV",
                       "libanon": "LB", "libya": "LY", "liecthenstein": "LI", "liettua": "LT", "luxemburg": "LU", "makedonia": "MK",
                       "malediivit": "MV", "malesia": "MY", "malta": "MT", "marokko": "MA", "meksiko": "MX", "moldova": "MD",
                       "monaco": "MC", "mongolia": "MN", "montenegro": "ME", "norja": "NO", "norsunluurannikko": "CI",
                       "pakistan": "PK", "panama": "PA", "paraguay": "PY", "peru": "PE", "portugali": "PT", "puerto rico": "PR",
                       "puola": "PL", "qatar": "QA", "ranska": "FR", "romania": "RO", "ruotsi": "SE", "saksa": "DE", "san marino": "SM",
                       "saudi-arabia": "SA", "serbia": "RS", "singapore": "SG", "slovakia": "SK", "slovenia": "SI", "suomi": "FI",
                       "sveitsi": "CH", "syyria": "SY", "taiwan": "TW", "tanska": "DK", "thaimaa": "TH", "tšekki": "CZ", "tunisia": "TN",
                       "turkki": "TR", "ukraina": "UA", "unkari": "HU", "uruguay": "UY", "uusi-seelanti": "NZ", "uzbekistan": "UZ",
                       "valko-venäjä": "BY", "vatikaanivaltio": "VA", "venezuela": "VE", "venäjä": "RU", "vietnam": "VN", "viro": "EE",
                       "yhdistynyt kuningaskunta": "UK", "yhdysvallat": "US"}
        code = convertdict.setdefault(country, "FI")
        return code

    def prettify(self, elem):
        """
        Return a pretty-printed XML string for the Element
        :param elem:
        :return:
        """
        rough_string = ET.tostring(elem, 'utf-8')
        reparsed = parseString(rough_string)
        return reparsed.toprettyxml(indent="\t")
