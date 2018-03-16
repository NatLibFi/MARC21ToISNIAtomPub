import logging
from pymarc import MARCReader, Record, Field, XMLWriter
from pymarc.exceptions import BaseAddressInvalid, RecordLeaderInvalid, BaseAddressNotFound, RecordDirectoryInvalid, NoFieldsFound, FieldNotFound
import sys, os, pprint
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
import xml.etree.cElementTree as ET
import random
import csv
import codecs
import xlrd

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
        :param include: records with this field
    """

    def __init__(self, inputfilename, xlsfilename, csvfilename, include=None):
        self.include = include
        self.infile = inputfilename
        self.xlsfile = xlsfilename
        self.csvfile = csvfilename
        self.delimiter = "¤" #delimiter used in CSV file
        self.currentId = 0

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

    def takerandomsample(self, outfile, n):
        """

        :param outfile:
        :param n:
        :return:
        """
        logging.info("Starting mrc to mrc random sample conversion...")

        with open(self.infile, 'rb') as fh:
            reader = MARCReader(fh, force_utf8=True, to_unicode=True)
            out = open(outfile, 'wb')
            my_randoms = random.sample(range(159367), n*4)
            for i, record in enumerate(reader):
                if any(f.tag != self.include for f in record.fields):
                    continue
                if i in my_randoms:
                    logging.info("Converting record.")
                    sys.stdout.write('.')
                    sys.stdout.flush()
                    if i % 5 == 0:
                        print("\rConverting", end="")
                    out.write(record.as_marc())
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

    def convert2ISNIRequestXML(self, dirname, dirmax=20, concat=False):
        """
        Converts MARC21 to ISNI XML Requests. Creates directory dirname, and creates XML Request files into the folder.
        dirmax default is 20, so it makes 20 request xml files per folder before creating a new one.
        :param dirname:
        :param dirmax:
        """

        self.concat = concat
        if self.include:
            print("Converting records with %s field" % self.include)
        logging.info("Starting mrc to isni request conversion...")
        # pp = pprint.PrettyPrinter(indent=2)
        with open(self.infile, 'rb') as fh, open(self.csvfile) as countryfile:
            self.countryreader = csv.reader(countryfile, delimiter=self.delimiter, quoting=csv.QUOTE_NONE)
            if not os.path.exists(dirname):
                os.mkdir(dirname)
            reader = MARCReader(fh, force_utf8=True, to_unicode=True)
            i = 1
            dirinc = 0
            xml = ""
            if self.concat:
                with open(dirname+"/concat_request.xml", 'ab+') as concat_file:
                    concat_file.write(bytes("<?xml version=\"1.0\" ?>\n<root>\n", "UTF-8"))
            
            """
            read Excel workbook of titles of authors with columns arranged by this order in the first worksheet: 
            asteriId, creationRole, title, publisher, date, ISBN, ISSN
            """
            print('Opening Excel workbook file...')
            book = xlrd.open_workbook(self.xlsfile)
            print('Workbook opened')
            sh = book.sheet_by_index(0)
            self.titles = {}
            resources = []
            id = 0
            total = 0
            count = 0
            for x in range (sh.nrows):
                resource = {}
                try:
                    if id != sh.cell_value(x, 0):
                        count = 0
                    if sh.cell_value(x, 1) != "":
                        resource.update({"creationRole": sh.cell_value(x, 1)})
                    if sh.cell_value(x, 2) != "":
                        resource.update({"title": sh.cell_value(x, 2)})
                    if sh.cell_value(x, 3) != "":
                        resource.update({"publisher": sh.cell_value(x, 3)})
                        #publisher is mandatory value, date is optional value in ISNI imprint tag
                        if sh.cell_value(x, 4) != "":
                            date = str(sh.cell_value(x, 4))
                            if date.endswith('.0'):
                                date = date[:-2]
                            resource.update({"date": date})
                    if sh.cell_value(x, 5) != "":
                        isbn = str(sh.cell_value(x, 5))
                        if isbn.endswith('.0'):
                            isbn = isbn[:-2]
                        resource.update({"identifierValue": isbn})
                        resource.update({"identifierType": "ISBN"})
                    if sh.cell_value(x, 6) != "":
                        issn = str(sh.cell_value(x, 6))
                        if issn.endswith('.0'):
                            issn = issn[:-2]
                        resource.update({"identifierValue": issn})
                        resource.update({"identifierType": "ISSN"})
                    if sh.cell_value(x, 0) in self.titles:
                        if count < 10:
                            resources.append(resource)
                            total += 1
                    else:
                        resources = []
                        resources.append(resource)
                        total += 1
                        self.titles[sh.cell_value(x, 0)] = resources
                    if sh.cell_value(x, 0) in self.titles:
                        count += 1
                    id = sh.cell_value(x, 0)
                except IndexError:
                    y = 0
            print('Total number of titles: ', total)

            #read country codes from CSV file:
            self.countries = {}
            for row in self.countryreader:
                if row[1] != "":
                    self.countries.update({row[0]: row[1]}) 

            try:
                for record in reader:
                    #if any(f.tag != self.include for f in record.fields):
                    #continue
                    logging.info("Converting record.")
                    #if self.include in record:
                    if any(f.tag == self.include for f in record.fields):
                        r = self.makeIsniRequest(record)
                        xml = parseString(ET.tostring(r, "utf-8")).toprettyxml()
                        if i % dirmax == 0:
                            dirinc += 1
                        subdir = dirname + "/" + str(dirinc).zfill(3)
                        if not (os.path.exists(subdir)) and not self.concat:
                            os.mkdir(subdir)
                        if self.concat:
                            xml = xml.replace("<?xml version=\"1.0\" ?>", "")
                            xmlfile = open(dirname+"/concat_request.xml", 'ab+')
                            xmlfile.write(bytes(xml, 'UTF-8'))
                            xmlfile.flush()
                            i += 1
                        else:
                            xmlfile = open(subdir + "/request_" + str(i).zfill(5) + ".xml", 'wb+')
                            xmlfile.write(bytes(xml, 'UTF-8'))
                            xmlfile.close()
                            i += 1
            except BaseAddressInvalid:
                print('Error next to line: ', self.currentId)
            except BaseAddressNotFound:
                print('Error next to line: ', self.currentId)
                
            if self.concat:
                with open(dirname+"/concat_request.xml", 'ab+') as concat_file:
                    concat_file.write(bytes("</root>", "UTF-8"))

            print("Conversion done for {} items".format(i))

    def makeIsniRequest(self, record):

        requestIdentifier = []
        personalName = []
        organisationTypes = []
        usageDateFrom = []
        usageDateTo = []
        #locationCountryCode = []
        externalInformation = []
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
        birthDate = []
        deathDate = []
        personalNameVariant = []
        organisationMainSubdivNames = []
        organisationVariantSubdivNames = []

        organisationVariants = []
        organisationMains = []

        relations = []

        for t in record.get_fields("410"):
            org = {"mainName": t['a']}
            if t['b']:
                subdivisions = []
                for index, s in enumerate(t.subfields):
                    if s is 'b':
                        subdivisions.append(t.subfields[index+1])
                org.update({"subdivisionName": subdivisions})
            organisationVariants.append(org)

        for t in record.get_fields("411"):
            org = {"mainName": t['a']}
            if t['b']:
                subdivisions = []
                for index, s in enumerate(t.subfields):
                    if s is 'b':
                        subdivisions.append(t.subfields[index + 1])
                org.update({"subdivisionName": subdivisions})
            organisationVariants.append(org)

        for t in record.get_fields("510"):
            isRelated = {}
            if t['w'] is "a":
                isRelated = {"relationType": "supersedes", "identityType": "organisation", "relationName": {"PPN": "", "organisationName": t['a']}}
                if t['b']:
                    isRelated.update({"relationName": {"PPN": "", "organisationName": t['a'], "subDivisionName": t['b']}})
                relations.append(isRelated)
            elif t['w'] is "b":
                isRelated = {"relationType": "isSupersededBy", "identityType": "organisation", "relationName": {"PPN": "", "organisationName": t['a']}}
                if t['b']:
                    isRelated.update({"relationName": {"PPN": "", "organisationName": t['a'], "subDivisionName": t['b']}})
                relations.append(isRelated)
            elif t['w'] is "t":
                isRelated = {"relationType": "isUnitOf", "identityType": "organisation", "relationName": {"PPN": "", "organisationName": t['a']}}
                if t['b']:
                    isRelated.update({"relationName": {"PPN": "", "organisationName": t['a'], "subDivisionName": t['b']}})
                relations.append(isRelated)
                
        #if '100' in record:
        if self.include == '100':
            for t in record.get_fields("400"):
                forename = ""
                #Split up personal name variants to surename/forename
                if "," in t['a']:
                    surname, forename = t['a'].split(',', 1)
                    surname = surname.replace(",", "")
                    forename = forename.replace(",", "")
                    surname = surname.strip()
                    forename = forename.strip()
                else:
                    surname = t['a']
                pvariant = {"nameUse": "public", "surname": surname}
                if forename:
                    pvariant.update({"forename": forename})
                personalNameVariant.append(pvariant)
                
        for t in record.get_fields("377"):
            for c in t.get_subfields("a"):
                languageOfIdentity.append(c)
        
        for t in record.get_fields("670"):
            information = {}
            """
            This might be used later, currently not required:
            if t['a']:
                information.update({"externalInformationSource": t['a']})
            if t['b']:
                information.update({"externalInformationInfo": t['b']})
            """    
            if t['u']:
                information.update({"externalInformationURI": t['u']})
            externalInformation.append(information)
            
        for field in record.fields:
            if field.tag == '100':
                personalName.append(record['100']['a'])
            elif field.tag == '035':
                requestIdentifier.append(record['035']['a'])
            elif field.tag == '001':
                requestIdentifier.append("(FI-ASTERI-N)"+record['001'].data)
                self.currentId = record['001'].data
            elif field.tag == '110':
                org = {"mainName": record['110']['a']}
                if record['110']['b']:
                    for index, subfield in enumerate(record['110'].subfields):
                        if subfield is 'b':
                            organisationMainSubdivNames.append(record['110'].subfields[index+1])
                organisationMains.append(org)
            elif field.tag == '368':
                organisationTypes.append(record['368']['a'])
            elif field.tag == '046':
            #Skip these subfields for the moment (the original data in s and t subfields contains dates in other forms than YYYY):
            #if record['046']['s']:
            #usageDateFrom.append(record['046']['s'])
            #if record['046']['t']:
            #usageDateTo.append(record['046']['t'])
                if record['046']['q'] and record['046']['q'].isdigit():
                    usageDateFrom.append(record['046']['q'])
                if record['046']['r'] and record['046']['r'].isdigit():
                    usageDateTo.append(record['046']['r'])
                if record['046']['f']:
                    birthDate.append(record['046']['f'])
                elif record['046']['g']:
                    deathDate.append(record['046']['g'])     
        
        requestxml = ET.Element("Request")
        idinfoxml = ET.SubElement(requestxml, "identityInformation")
        identityxml = ET.SubElement(idinfoxml, "identity")
        requestoridxml = ET.SubElement(idinfoxml, "requestorIdentifierOfIdentity")
        if self.include == "110" :
            organisationxml = ET.SubElement(identityxml, "organisation")
            organisationtypexml = ET.SubElement(organisationxml, "organisationType")
            #get the first organisation type from the list, because ISNI record cannot have more than one type:
            if organisationTypes:
                organisationtypexml.text = self.converttoniso(organisationTypes[0])
            else:
                organisationtypexml.text = "Other to be defined"
            if usageDateFrom:
                usagedatefromxml = ET.SubElement(organisationxml, "usageDateFrom")
                #get the first date from the list, because ISNI record cannot have more than one date:
                usagedatefromxml.text = usageDateFrom[0]
            if usageDateTo:
                usagedatetoxml = ET.SubElement(organisationxml, "usageDateTo")
                #get the first date from the list, because ISNI record cannot have more than one date:
                usagedatetoxml.text = usageDateTo[0]
            resourcexml = ET.SubElement(organisationxml, "resource")
        
        if languageOfIdentity:
            for c in languageOfIdentity:
                languagexml = ET.SubElement(idinfoxml, "languageOfIdentity")
                languagexml.text = c
                
        if externalInformation:
            for c in externalInformation:
                if "externalInformationURI" in c:
                    externalinformationxml = ET.SubElement(idinfoxml, "externalInformation")
                    urixml = ET.SubElement(externalinformationxml, "URI")
                    urixml.text = c['externalInformationURI']
                    """
                    externalInformationURI is currenly the only mandatory element in externalInformation:
                    if "externalInformationSource" in c:
                        externalinformationsourcexml = ET.SubElement(externalinformationxml, "source")
                        print(c['externalInformationSource'])
                        externalinformationsourcexml.text = c['externalInformationSource']
                    if "externalInformationInfo" in c:
                        externalinformationinfoxml = ET.SubElement(externalinformationxml, "information")
                        print(c['externalInformationInfo'])
                        externalinformationinfoxml.text = c['externalInformationInfo']
                    """
                
        if requestIdentifier:
            for c in list(set(requestIdentifier)):
                identifier = ET.SubElement(requestoridxml, "identifier")
                identifier.text = c
        
        #not in use currently, ISNI identifier should be placed here:
        if otherIdentifier:
            otheridentifierxml = ET.SubElement(idinfoxml, "otherIdentifierOfIdentity")
            for c in list(set(otherIdentifier)):
                otherid = ET.SubElement(otheridentifierxml, "identifier")
                otherid.text = c
        
        #if self.skip != "100" :
        if '100' in record:
            if personalName:
                personorfictionxml = ET.SubElement(identityxml, "personOrFiction")
                personalNamexml = ET.SubElement(personorfictionxml, "personalName")
                nameusexml = ET.SubElement(personalNamexml, "nameUse")
                nameusexml.text = "public"
                # We need to split up the name into surname/forename, if it's not splittable (i.e. doesn't have a comma separator)
                # put the whole name into surname field. For clarity this could be done the same way as personalNameVariants
                if "," in list(set(personalName))[0]:
                    surname, forename = list(set(personalName))[0].split(',', 1)
                    surname = surname.replace(",", "")
                    forename = forename.replace(",", "")
                    surname = surname.strip()
                    forename = forename.strip()
                    surnamexml = ET.SubElement(personalNamexml, "surname")
                    surnamexml.text = surname
                    if forename:
                        forenamexml = ET.SubElement(personalNamexml, "forename")
                        forenamexml.text = forename
                else:
                    surname = list(set(personalName))[0]
                    surnamexml = ET.SubElement(personalNamexml, "surname")
                    surnamexml.text = surname
                #Prune birthdates of unnecessary characters
                if birthDate:
                    for c in birthDate:
                        birthdatexml = ET.SubElement(personorfictionxml, "birthDate")
                        bd = str(''.join(list(filter(str.isdigit, c))))
                        birthdatexml.text = bd
                if deathDate:
                    for c in deathDate:
                        deathdatexml = ET.SubElement(personorfictionxml, "deathDate")
                        dd = str(''.join(list(filter(str.isdigit, c))))
                        deathdatexml.text = dd

        if organisationMains:
            orgnamexml = ET.SubElement(organisationxml, "organisationName")
            mainnamexml = ET.SubElement(orgnamexml, "mainName")
            for c in organisationMains:
                mainnamexml.text = c['mainName']
                if organisationMainSubdivNames:
                    for t in organisationMainSubdivNames:
                        subdivnamexml = ET.SubElement(orgnamexml, "subdivisionName")
                        subdivnamexml.text = t

        try:
            if record['001'].data in self.countries.keys():
                locationxml = ET.SubElement(organisationxml, "location")
                countrycodexml = ET.SubElement(locationxml, "countryCode")
                countrycodexml.text = self.countries[record['001'].data]
        except AttributeError:
            print ("error in location", self.currentId)
        
        if relations:
            for c in relations:
                isrelatedxml = ET.SubElement(requestxml, "isRelated")
                isrelatedxml.set("identityType", c['identityType'])
                relationtypexml = ET.SubElement(isrelatedxml, "relationType")
                relationtypexml.text = c['relationType']
                relationnamexml = ET.SubElement(isrelatedxml, "relationName")
                relorgnamexml = ET.SubElement(relationnamexml, "organisationName")
                relorgmainxml = ET.SubElement(relorgnamexml, "mainName")
                relorgmainxml.text = c['relationName']['organisationName']
                if c['relationName'].get('subDivisionName') is not None:
                    subdivnamexml = ET.SubElement(relorgnamexml, "subdivisionName")
                    subdivnamexml.text = c['relationName']['subDivisionName']

        if organisationVariants:
            for c in organisationVariants:
                try:
                    orgnamevariantxml = ET.SubElement(organisationxml, "organisationNameVariant")
                    orgnamevariantnamexml = ET.SubElement(orgnamevariantxml, "mainName")
                    orgnamevariantnamexml.text = c['mainName']
                    if "subdivisionName" in c:
                        for s in c['subdivisionName']:
                            subdivnamexml = ET.SubElement(orgnamevariantxml, "subdivisionName")
                            subdivnamexml.text = s
                except UnboundLocalError:
                    organisationxml = ET.SubElement(identityxml, "organisation")
                    orgnamevariantxml = ET.SubElement(organisationxml, "organisationNameVariant")
                    orgnamevariantnamexml = ET.SubElement(orgnamevariantxml, "mainName")
                    orgnamevariantnamexml.text = c['mainName']
                    if "subdivisionName" in c:
                        for s in c['subdivisionName']:
                            subdivnamexml = ET.SubElement(orgnamevariantxml, "subdivisionName")
                            subdivnamexml.text = s

        if self.include == "100" :                    
            if personalNameVariant:
                for c in personalNameVariant:
                    try:
                        personnamevariantxml = ET.SubElement(personorfictionxml, "personalNameVariant")
                        personnamevariantsurname = ET.SubElement(personnamevariantxml, "surname")
                        personnamevariantsurname.text = c['surname']
                        personnamevariantuse = ET.SubElement(personnamevariantxml, "nameUse")
                        personnamevariantuse.text = c['nameUse']
                        if "forename" in c:
                            personnamevariantforename = ET.SubElement(personnamevariantxml, "forename")
                            personnamevariantforename.text = c['forename']
                    except UnboundLocalError:
                        personorfictionxml = ET.SubElement(identityxml, "personOrFiction")
                        personnamevariantxml = ET.SubElement(personorfictionxml, "personalNameVariant")
                        personnamevariantsurname = ET.SubElement(personnamevariantxml, "surname")
                        personnamevariantsurname.text = c['surname']
                        personnamevariantuse = ET.SubElement(personnamevariantxml, "nameUse")
                        personnamevariantuse.text = c['nameUse']
                        if "forename" in c:
                            personnamevariantforename = ET.SubElement(personnamevariantxml, "forename")
                            personnamevariantforename.text = c['forename']

        try:
            if record['001'].data in self.titles.keys():
                resources = self.titles[record['001'].data]
                for c in resources:
                    #title is the only mandatory element in resource:
                    if "title" in c:
                        titleofworkxml = ET.SubElement(resourcexml, "titleOfWork")
                        title = ET.SubElement(titleofworkxml, "title")
                        title.text = c['title']
                        if "creationRole" in c:
                            creationrole = ET.SubElement(resourcexml, "creationRole")
                            creationrole.text = c['creationRole']
                        #publisher is mandatory in imprint element, date is optional:
                        if "publisher" in c:
                            imprintxml = ET.SubElement(titleofworkxml, "imprint")
                            publisher = ET.SubElement(imprintxml, "publisher")
                            publisher.text = c['publisher']
                            if "date" in c:
                                date = ET.SubElement(imprintxml, "date")
                                date.text = c['date']
                        if "identifierValue" in c:
                            identifierxml = ET.SubElement(titleofworkxml, "identifier")
                            identifierValue = ET.SubElement(identifierxml, "identifierValue")
                            identifierValue.text = c['identifierValue']
                            identifierType = ET.SubElement(identifierxml, "identifierType")
                            identifierType.text = c['identifierType']
        except AttributeError:
            print ("error in titles", self.currentId)
            
        return requestxml

    @staticmethod
    def converttoniso(organisationType):
        convertdict = {"järjestö": "Not for Profit Organization",  "kerho": "Not for Profit Organization", "ryhmä": "Not for Profit Organization",
                       "sukuseura": "Not for Profit Organization", "yhdistys": "Not for Profit Organization", "yhtiö": "For Profit Corporation",
                       "yritys": "Not for Profit Organization", "virasto": "Government Agency or Department", "kuoro": "Musical group or band",
                       "orkesteri": "Musical group or band", "yhtye": "Musical group or band"}
        if organisationType in convertdict.keys():
            return convertdict[organisationType]
        return "Other to be defined"
    
    @staticmethod
    def converttoiso(country):
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
                       "yhdistynyt kuningaskunta": "GB", "yhdysvallat": "US"}
        print(convertdict.keys())               
        if country in convertdict.keys():
            return convertdict[country]
        return "XX"

    def prettify(self, elem):
        """
        Return a pretty-printed XML string for the Element
        :param elem:
        :return:
        """
        rough_string = ET.tostring(elem, 'utf-8')
        reparsed = parseString(rough_string)
        return reparsed.toprettyxml(indent="\t")
