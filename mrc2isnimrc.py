import logging
from pymarc import MARCReader, Record, Field
import sys


class MARC21ToISNIMARC:
    """
        A class to transform mrc binary files from MARC21 format to ISNIMARC format.
        Takes inputfilename and outputfilename as parameters for infile and outfile fields,
        which are used for reading from and writing to the mrc files.

        converter = MARC21ToISNIMARC("inputfile.mrc", "outputfile.mrc")

        Convert using the conv method

        converter.conv()
    """


    def __init__(self, inputfilename, outputfilename):
        self.infile = inputfilename
        self.outfile = outputfilename

    def __str__(self):
        """
        In a string context MARC21ToISNIMARC will return basic info of the object itself
        :return str:
        """
        return "Converting file %s to %s, <%s object at %s, %s bytes>" % (self.infile, self.outfile, self.__class__.__name__,
                                                              hex(id(self)), self.__sizeof__())

    def __sizeof__(self):
        return sys.getsizeof(MARC21ToISNIMARC)

    def convert(self):
        """
        This method opens the specified files (input and output), reads infile, converts it and writes it into the outfile
        :return:
        """
        logging.info("Starting conversion...")
        with open(self.infile, 'rb') as fh:
            reader = MARCReader(fh, force_utf8=True, to_unicode=True)
            out = open(self.outfile, 'wb')
            i = 0
            for record in reader:
                logging.info("Converting record.")
                sys.stdout.write('.')
                sys.stdout.flush()
                if i % 5 == 0:
                    print("\rConverting", end="")
                r = self.make_record(record)
                out.write(r.as_marc())
                i += 1
            out.close()
            print("\rConversion done.")

    def make_record(self, record):
        """
        Converts the record the pymarcs MARCReader has read to ISNIMARC format.
        Takes a whole record as a parameter and returns the converted record.
        :param record:
        :return:
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

        newrecord.add_field(Field(tag='922', indicators=['\\', '\\'], subfields=['a', 'FI'], marctype="isni"))
        return newrecord

