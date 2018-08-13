#! /usr/bin/env python3

# Fetcher for Eurostat data

"""
Fetch series from Eurostat, the statistical office of the European Union, using bulk download.

Database:
- http://ec.europa.eu/eurostat/data/database
BUlk download instructions:
- http://ec.europa.eu/eurostat/data/bulkdownload
Bulk download facility:
- http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing

"""
import sys
import os
import argparse
import io
import logging
import requests
from pathlib import Path
from lxml import etree

log = logging.getLogger(__name__)

XML_TOC_URL = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents.xml"
XML_TOC_FILENAME= "table_of_contents.xml"


class Downloader():
    def __init__(self, force_full_reload=False, target_dir='./', loglevel=logging.INFO):
        self.force_full_reload = force_full_reload
        self.target_dir = target_dir
        self.xml_toc_file_location = target_dir + XML_TOC_FILENAME
        self.nsmap = dict(nt='urn:eu.europa.ec.eurostat.navtree')

    def download_new_toc(self):
        log.info("Starting file Table of Contents XML file download...")
        response = requests.get(XML_TOC_URL)
        log.info(">> download finished.")
        parser = etree.XMLParser(remove_blank_text=True)
        xml_toc = etree.fromstring(response.content, parser=parser)
        self.xml_toc = xml_toc
        with open(self.xml_toc_file_location, 'wb') as xml_file:
            etree.ElementTree(xml_toc).write(xml_file, encoding='utf-8', pretty_print=True, xml_declaration=True)

    def load_existing_toc(self):
        if Path(self.xml_toc_file_location).is_file() and not self.force_full_reload:
            parser = etree.XMLParser(remove_blank_text=True)
            self.xml_toc_old = etree.parse(self.xml_toc_file_location, parser=parser)
        else:
            self.xml_toc_old = None

    def update_toc(self):
        self.load_existing_toc()
        self.download_new_toc()

    def xml_ds_to_update(self):
        last_update_by_ds = {}
        for ds in self.xml_toc_old.iterfind('.//nt:leaf[@type="dataset"]', namespaces=self.nsmap):
            ds_code = ds.findtext("nt:code", namespaces=self.nsmap)
            ds_last_update = ds.findtext("nt:lastUpdate", namespaces=self.nsmap)
            last_update_by_ds[ds_code] = ds_last_update

        for ds in self.xml_toc.iterfind('.//nt:leaf[@type="dataset"]', namespaces=self.nsmap):
            if self.xml_toc_old is None:
                yield ds
            else:
                ds_code = ds.findtext("nt:code", namespaces=self.nsmap)
                ds_earlier_update = last_update_by_ds.get(ds_code)
                if ds_earlier_update is None:
                    yield ds
                else:
                    ds_last_update = ds.findtext("nt:lastUpdate", namespaces=self.nsmap)
                    if ds_last_update != ds_earlier_update:
                        yield ds

    def download_ds(self):
        datasets = set()
        datastructures = set()
        for ds in self.xml_ds_to_update():
            ds_code = ds.findtext('./nt:code', namespaces=self.nsmap)
            ds_url = ds.findtext('./nt:downloadLink[@format="sdmx"]', namespaces=self.nsmap)
            if ds_url:
                datasets.add((ds_code, ds_url))
            dstr_url = ds.findtext('./nt:metadata[@format="sdmx"]', namespaces=self.nsmap)
            if dstr_url:
                datastructures.add(dstr_url)

    
    


def main():
    d = Downloader()
    d.update_toc()


if __name__ == '__main__':
    sys.exit(main())


