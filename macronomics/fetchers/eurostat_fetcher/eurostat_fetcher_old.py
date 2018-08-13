# -*- coding: utf-8 -*-

import logging
import requests
from lxml import etree
from pathlib import Path
from io import StringIO
import csv
import gzip
import io

from pandasdmx import Request


from macronomics.fetchers.base_fetcher import BaseFetcher


GIT_PARENT_LOCATION = '/home/mattange/Downloads'
TEMP_PARENT_LOCATION = '/home/mattange/Downloads'
NAME = 'eurostat'
CSV_NAN_REPRESENTATION = 'na'

log = logging.getLogger(__name__)
XML_TOC_URL = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=table_of_contents.xml"
XML_TOC_FILE= "table_of_contents.xml"

URL = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing"


logging.basicConfig(level=logging.INFO)

class SDMXFetcher(BaseFetcher):
    pass

class EurostatFetcher(SDMXFetcher):
    
    _sdmx_loader = Request('ESTAT')
    _nsmap = dict(nt='urn:eu.europa.ec.eurostat.navtree')
    
    _GEO = ['AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES',
            'EU28', 'EU27', 'EA19', 'FI', 'FR', 'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'ME', 
            'MK', 'MT', 'NL', 'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'TR', 'UK']

    _S_ADJ = ['NSA','SA']
    
    _FREQ = ['A','H','Q','M','S','W']
    
    
    def __init__(self, name=NAME, parent_location=GIT_PARENT_LOCATION):
        
        self._name = name
        self._base_url = URL
        
        self._parent_location = parent_location
        self._repo_location = parent_location + "/" + name
        self._temp_location = TEMP_PARENT_LOCATION + "/" + name
        self._xml_toc_file = XML_TOC_FILE 
        self._xml_toc_file_location = self._repo_location + "/" + self._xml_toc_file
        Path(self._temp_location).mkdir(exist_ok=True)
        Path(self._repo_location).mkdir(exist_ok=True)
        try:
            self.load_existing_repo()
        except ValueError:
            self.initialize_repo()         

    def _download_file(self, params, stream):
        rsp = requests.get(self._base_url,params,stream=stream)
        if rsp.ok:
            file_path = self._repo_location + '/' + params['file'].split('/')[-1]
            with open(file_path, 'wb') as f:
                for chunk in rsp.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                f.close()
                self.repo.stage([file_path])
        return rsp

    def _dictionary_file(self, params):
        rsp = self._download_file(params, False)
        if rsp.ok:
            tsv_file_io = StringIO(rsp.text)
            reader = csv.reader(tsv_file_io,delimiter='\t')
            return dict([(r[0],r[1]) for r in reader])

    def _dimensions_list(self):
        log.info("Downloading and updating the dimensions list...")
        params = {"sort":"1","file":"dic/en/dimlst.dic"}
        self._dimlst = self._dictionary_file(params)
        
    def _load_existing_toc(self):
        if Path(self._xml_toc_file_location).is_file():
            log.info("Loading up existing Table of Contents XML file...")
            parser = etree.XMLParser(remove_blank_text=True)
            self.xml_toc_old = etree.parse(self._xml_toc_file_location, parser=parser)
        else:
            log.info("No existing Table of Contents XML file. Setting to None.")
            self.xml_toc_old = None
    
    def _update_toc(self):
        self._load_existing_toc()
        
        log.info("Starting file Table of Contents XML file download...")
        response = requests.get(XML_TOC_URL)
        log.info(">> download finished.")
        parser = etree.XMLParser(remove_blank_text=True)
        xml_toc = etree.fromstring(response.content, parser=parser)
        self.xml_toc = etree.ElementTree(xml_toc)
        with open(self._xml_toc_file_location, 'wb') as xml_file:
            self.xml_toc.write(xml_file, encoding='utf-8', pretty_print=True, xml_declaration=True)
            xml_file.close()
        self.repo.stage([self._xml_toc_file])
            
    def _datasets_to_update(self, forceUpdateAll=False):
        
        if forceUpdateAll:
            for ds in self.xml_toc.iterfind('.//nt:leaf[@type="dataset"]', namespaces=self._nsmap):
                yield ds
        else:
            if self.xml_toc_old is not None:
                last_update_by_ds = {}
                for ds in self.xml_toc_old.iterfind('.//nt:leaf[@type="dataset"]', namespaces=self._nsmap):
                    ds_code = ds.findtext("nt:code", namespaces=self._nsmap)
                    ds_last_update = ds.findtext("nt:lastUpdate", namespaces=self._nsmap)
                    last_update_by_ds[ds_code] = ds_last_update
                
            for ds in self.xml_toc.iterfind('.//nt:leaf[@type="dataset"]', namespaces=self._nsmap):
                ds_code = ds.findtext("nt:code", namespaces=self._nsmap)
                if self.xml_toc_old is None:
                    yield ds
                else:
                    ds_earlier_update = last_update_by_ds.get(ds_code)
                    if ds_earlier_update is None:
                        yield ds
                    else:
                        ds_last_update = ds.findtext("nt:lastUpdate", namespaces=self._nsmap)                    
                        if ds_last_update != ds_earlier_update:
                            yield ds
    
    
    def update_content(self, forceUpdateAll=False):
        ds_iter = self._datasets_to_update(forceUpdateAll)
        
        for ds in ds_iter:
            ds_code = ds.findtext("nt:code", namespaces=self._nsmap)
            #ds_last_update = ds.findtext("nt:lastUpdate", namespaces=self._nsmap)
            #ds_metadata = ds.findtext('nt:metadata[@format="sdmx"]', namespaces=self._nsmap)
            #ds_url_tsv = ds.findtext('nt:downloadLink[@format="tsv"]', namespaces=self._nsmap)
            #ds_url_sdmx = ds.findtext('nt:downloadLink[@format="sdmx"]', namespaces=self._nsmap)
            
            
            log.info('Dowloading dataset {}'.format(ds_code))
            #SHORT APPROACH TO DOWNLOAD DATA AS TSV ONLY HAS SERIES
            #log.info('...tsv file...')
            #response = requests.get(ds_url_tsv)

            #LONG APPROACH TO DOWNLOAD DATA AS SDMX-->VERY INEFFICIENT IT SEEMS
            #BETTER TO DOWNLOAD IN ZIP, THEN UNZIP AND THEN READ FROM FILES?
            log.info('...sdmx information...'.format(ds_code))
            dsd_code = 'DSD_' + ds_code
            cs_code = 'CS_' + dsd_code
            dsd_response = self._sdmx_loader.datastructure(resource_id=dsd_code)
            dsd = dsd_response.msg.datastructure[dsd_code]
            dsd_df = dsd_response.write()
            cs = dsd_response.msg.conceptscheme[cs_code]
            cs_id_list = [c.id for c in cs.aslist()]
            cl = dsd_response.msg.codelist
            data_response = self._sdmx_loader.data(resource_id=ds_code, dsd=dsd)
            data = data_response.data
            series_l = list(data.series)
            series_df = data_response.write(series_l)
            #this is because the timeseries are in reverse order otherwise
            series_df.sort_index(inplace=True) 
            idx_multi = series_df.columns
            #drop levels that are unique in the multi-index
            lvl_to_drop = [i for i in range(len(idx_multi.levels)) if len(idx_multi.levels[i])==1]
            idx_multi = idx_multi.droplevel(lvl_to_drop)
            if 'INDIC' in idx_multi.names:
                idx_multi.swaplevel('INDIC',0)  #place INDIC first
            if 'INDICATOR' in idx_multi.names:
                idx_multi.swaplevel('INDICATOR',0)  #place INDICATOR first
            series_df.columns = idx_multi
            idx_multi_joined = ['_'.join(col).strip() for col in idx_multi.values]
            series_df.columns = idx_multi_joined
            series_df.to_csv(self._repo_location + ds_code +'.csv', na_rep="na")

