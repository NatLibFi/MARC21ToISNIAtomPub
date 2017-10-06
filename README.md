# MARC21ToISNIMARC
A tool to transform mrc binary files from MARC21 format to ISNIMARC format.


#### From MARC to ISNI request

```python
from mrc2isnimrc import MARC21ToISNIMARC
def main():

    # Initialize the converter with the marc file and default country code FI
    converter = MARC21ToISNIMARC("my_marc_file.mrc", "FI")

    # Give the output filename and the size of the sample
    converter.takerandomsample("my_random_sample.mrc", 1000)

    # Skip the items that have the 110 field (organisations)
    isnireq = MARC21ToISNIMARC("my_random_sample.mrc", skip="110")

    # Give path where to save concatenated XML file
    isnireq.convert2ISNIRequestXML("my/path", concat=True)

    # From the same sample we can save the requests one by one to a directory
    # dirmax sets the maximum ISNI Request XML files per subdirectory
    isnireq.convert2ISNIRequestXML("my/path", dirmax=100)
```


#### From aleph seq to mrc

I added a small conversion script to convert aleph sequentials to ISO2709 marc format.
Install dependencies with `npm install`. and run the script with `node --max-old-space-size=4096 index.js`
