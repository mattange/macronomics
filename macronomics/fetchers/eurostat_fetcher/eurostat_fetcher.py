# -*- coding: utf-8 -*-

import logging
import requests
from lxml import etree
from pathlib import Path
from io import StringIO, BytesIO
import csv
from gzip import GzipFile
from zipfile import ZipFile
from datetime import datetime
import pickle

import pandas as pd

from macronomics.fetchers.base_fetcher import BaseFetcher

from sqlalchemy import bindparam

log = logging.getLogger(__name__)

class SDMXFetcher(BaseFetcher):
    
    _freq_to_pd = {
            #this is valid for Eurostat frequencies
            'A': 'A',
            'S': '2Q',
            'Q': 'Q',
            'M': 'M',
            'W': 'W',
            'B': 'W',
            'D': 'D',
            'H': 'H',
            'N': 'T'
            }
    
    @classmethod
    def _get_nsmap(cls, xml):
        nsmap = xml.getroot().nsmap
        nsmap.update({'xml':'http://www.w3.org/XML/1998/namespace'}) #appears missing
        return nsmap
    
    @classmethod
    def _interpret_dsd(cls, xml_dsd):
        nsmap = cls._get_nsmap(xml_dsd)
        
        dimensions = {}
        attributes = {}
        codelists = {}
        concepts = {}
        t_dimension = None
        pri_measure = None
        
        def iterfind(xml_dsd, descriptor):
            d = {}
            it = xml_dsd.iterfind(descriptor, namespaces=nsmap)
            for v in it:
                d[v.get('id').upper()] = v.xpath('.//Ref[@class="Codelist"]', namespaces=nsmap)[0].get('id').upper()
            return d
        dimensions = iterfind(xml_dsd, './/str:Dimension')
        attributes = iterfind(xml_dsd, './/str:Attribute')
        
        #assume there is only one time dimension and pick the first 
        elem = xml_dsd.find('.//str:TimeDimension[@id]', namespaces=nsmap)
        if elem is not None:
            t_dimension = elem.get('id').upper()
            
        #finds the primary measure description based on where the property id exists
        elem = xml_dsd.find('.//str:PrimaryMeasure[@id]', namespaces=nsmap)
        if elem is not None:
            pri_measure = elem.get('id').upper()
            
        it = xml_dsd.iterfind('.//str:Concept', namespaces=nsmap)
        for v in it:
            concepts[v.get('id').upper()] = v.xpath('.//com:Name[@xml:lang="en"]', namespaces=nsmap)[0].text
            
        it = xml_dsd.iterfind('.//str:Codelist', namespaces=nsmap)
        for v in it:
            it2 = v.iterfind('.//str:Code', namespaces=nsmap)
            codelists[v.get('id').upper()] = dict([(cd.get('id').upper(),
                     cd.find('.//com:Name[@xml:lang="en"]', namespaces=nsmap).text) for cd in it2])
    
        return dimensions, attributes, t_dimension, pri_measure, concepts, codelists
        
    
       
        
    
#    @classmethod
#    def _bulk_store_series(cls, ss, conn, tbl, update=True):
#        
#        print('THIS IS A TEST IMPLEMENTATION')
#        
#        def fdata(s):
#            return {'name': s.name,
#                 'pd_series': pickle.dumps(s)}
#
#        engine = conn.engine
#        if update:    
#            compiled = tbl.update().where(tbl.c.name==bindparam('name')).values(
#                    pd_series=bindparam('pd_series')).compile(dialect=engine.dialect)
#        else:
#            compiled = tbl.insert().values(
#                    name=bindparam('name'),
#                    pd_series=bindparam('pd_series')).compile(dialect=engine.dialect)
#        
#        
#        raw_conn = conn.connection.connection
#        cursor = raw_conn.cursor()
#        args = [fdata(s) for s in ss]
#        cursor.executemany(str(compiled),args)
#        raw_conn.commit()
#
    
#    @classmethod
#    def _bulk_store(cls, xml_data, xml_dsd, conn, tbl):
#        
#        print('THIS IS A TEST IMPLEMENTATION')
#        
#        dimensions, attributes, t_dimension, f_dimension, pri_measure, concepts, codelists = cls._interpret_dsd_zip(xml_dsd)
#        nsmap = cls._get_nsmap(xml_data)
#        ito = xml_data.xpath('.//data:Obs', namespaces={'data':nsmap['data']})
#        
#        
#        def ffloat(x):
#            try:
#                xx = float(x)
#            except TypeError:
#                xx = None
#            return xx
#        
#        def fdata(o):
#            per_s = o.get(t_dimension).split('-Q')
#            year = int(per_s[0])
#            qtr = int(per_s[1])
#            month = qtr*3
#            dt = (31, 30, 30, 31)
#            d = datetime(year, month, dt[qtr-1]).date()
#            v = ffloat(o.get(pri_measure))
#            s = o.getparent()
#            d = {'refdate': d,
#                 'indicator': s.get('na_item'),
#                 'geo': s.get('geo'),
#                 'value': v,
#                 'attribute': o.get('OBS_STATUS')
#                 }
#            
#            return d
#        
#        engine = conn.engine
#        compiled = tbl.insert().values(
#                refdate=bindparam('refdate'),
#                indicator=bindparam('indicator'),
#                geo=bindparam('geo'),
#                value=bindparam('value'),
#                attribute=bindparam('attribute')).compile(dialect=engine.dialect)
#        raw_conn = conn.connection.connection
#        cursor = raw_conn.cursor()
#        args = [fdata(o) for o in ito]
#        cursor.executemany(str(compiled),args)
#        raw_conn.commit()
        
    
    @classmethod
    def _series(cls, xml_data, xml_dsd):
        
        dimensions, attributes, t_dimension, f_dimension, pri_measure, concepts, codelists = cls._interpret_dsd_zip(xml_dsd)
        nsmap = cls._get_nsmap(xml_data)
        it = xml_data.xpath('.//data:Series', namespaces={'data':nsmap['data']})
        
        def ffloat(x):
            try:
                xx = float(x)
            except TypeError:
                xx = None
            return xx

        for s in it:
            codename = "_".join([s.get(k) for k in dimensions.keys()])
            ito = s.xpath('data:Obs', namespaces={'data':nsmap['data']})
            freq = cls._freq_to_pd[s.get(f_dimension)]
            if freq is not None:
                v = [o.get(t_dimension) for o in ito]
                idx = pd.DatetimeIndex(start=v[0], periods=len(v), freq=freq, name='period')
            else:
                idx = pd.DatetimeIndex(v)
            pds = pd.Series([ffloat(o.get(pri_measure)) for o in ito], index=idx, name=codename)
            pds.dropna(inplace=True)
            if len(pds) > 0:
                yield pds
            else:
                continue
    
    

class EurostatFetcher(SDMXFetcher):
    
    _nsmap = dict(nt='urn:eu.europa.ec.eurostat.navtree')
    _base_bulk_url = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing"
    _base_dsd_url = "http://ec.europa.eu/eurostat/SDMX/diss-web/rest/datastructure/ESTAT"
    _name = "eurostat"

    @classmethod
    def _interpret_dsd_zip(cls, xml_dsd):
        nsmap = cls._get_nsmap(xml_dsd)
        
        #get header
        #he = xml_dsd.find('Header', namespaces=nsmap)

        #get keyfamilies (dimensions, attributes, timedimension, observations)
        dimensions = {}
        attributes = {}
        kf = xml_dsd.find('KeyFamilies', namespaces=nsmap)
        it = kf.iterfind('.//structure:Dimension', namespaces=nsmap)
        for v in it:
            dimensions[v.get('conceptRef').upper()] = v.get('codelist')
        f_dimension = kf.xpath('.//structure:Dimension[@isFrequencyDimension]', 
                               namespaces={'structure':nsmap['structure']})[0].get('conceptRef')
        
        it = kf.iterfind('.//structure:Attribute', namespaces=nsmap)
        for v in it:
            attributes[v.get('conceptRef').upper()] = v.get('codelist')
        
        t_dimension = kf.find('.//structure:TimeDimension', namespaces=nsmap).get('conceptRef').upper()
        pri_measure = kf.find('.//structure:PrimaryMeasure', namespaces=nsmap).get('conceptRef').upper()
        
        #get all concepts into a dictionary, with related english language explanation
        co = xml_dsd.find('Concepts', namespaces=nsmap)
        it = co.iterfind('.//structure:Concept', namespaces=nsmap)
        concepts = dict([(c.get('id').upper(),
                          c.find('.//structure:Name[@xml:lang="en"]',namespaces=nsmap).text) for c in it])
        
        #get codelists related to the concepts
        codelists = dict([(k,None) for k in concepts.keys()])
        cl = xml_dsd.find('CodeLists', namespaces=nsmap)
        it = cl.iterfind('.//structure:CodeList', namespaces=nsmap)
        for v in it:
            n = v.get('id').upper()
            it2 = v.iterfind('.//structure:Code', namespaces=nsmap)
            codelists[n] = dict([(cd.get('value').upper(), 
                     cd.find('.//structure:Description[@xml:lang="en"]',namespaces=nsmap).text) for cd in it2])
        
        return dimensions, attributes, t_dimension, f_dimension, pri_measure, concepts, codelists

    @classmethod
    def _interpret_dataset_sdmx_files(cls, f_dsd, f_data):
        parser = etree.XMLParser(remove_blank_text=True)
        xml_dsd = etree.parse(f_dsd, parser=parser)
        
        dimensions, attributes, t_dimension, f_dimension, pri_measure, concepts, codelists \
            = cls._interpret_dsd_zip(xml_dsd)
        
        for event, elem in etree.iterparse(f_data, events=("start","end"), tag="data:Series"):
            if event == "end":
                print(elem.get('id'))
        
     


    #
    # Dictionaries downloads: dimensions and codes lists
    #
    def _dl_dictionary(self, params):
        fb = self.download_file(self._base_bulk_url, params, stream=True)
        try:
            tsv_file_io = StringIO(fb.getvalue().decode('UTF-8'))
            reader = csv.reader(tsv_file_io, delimiter='\t')
            d = dict([(r[0],r[1]) for r in reader])
        except (UnicodeDecodeError, IndexError):
            d = None
        return d
    
    def _dl_dimensions_list(self):
        log.info("Downloading the dimensions list.")
        params = {"file":"dic/en/dimlst.dic"}
        self._dimlst = self._dl_dictionary(params)
    
    def _dl_codes_list(self):
        if not hasattr(self, '_dimlst'):
            self._dl_dimensions_list()
        self._codelst = dict([(k,None) for k in self._dimlst.keys()])
        for k in self._dimlst.keys():
            log.info("Downloading the codes list for {}.".format(k))
            params = {"file":"dic/en/" + k.lower() +".dic"}
            try:
                self._codelst[k] = self._dl_dictionary(params)
            except RuntimeError:
                #some dimensions do not have codelists (at rare times it is
                #correct, at other times it's clearly a mistake)
                #proceed and leave None for that dimension
                continue

    
    #
    # File objects downloads
    #
    def _dl_dataset_sdmx_zip_file(self, ds_code):
        params = {"file":"data/" + ds_code + ".sdmx.zip"}
        fb = self.download_file(self._base_bulk_url, params, 
                                stream=True, local_repo_file=params["file"])
        return ZipFile(fb)

    def _dl_dataset_tsv_gz_file(self, ds_code):
        params = {"file":"data/" + ds_code + ".tsv.gz"}
        fb = self.download_file(self._base_bulk_url, params, 
                                stream=True, local_repo_file=params["file"])
        return GzipFile(fileobj=fb)
    
    def _dl_dataset_dsd_file(self, ds_code):
        params = None
        url = self._base_dsd_url + "/DSD_" + ds_code
        fb = self.download_file(url, params, stream=True)
        return fb

    def _dl_dataset_sdmx_files(self, ds_code):
        zf = self._dl_dataset_sdmx_zip_file(ds_code)
        nl = zf.namelist()
        n_dsd = None
        n_data = None
        for n in nl:
            if n.find('dsd.xml') >= 0:
                n_dsd = n
            elif n.find('sdmx.xml') >= 0:
                n_data = n
        if n_dsd is None or n_data is None:
            raise RuntimeError("The sdmx.zip file for dataset {} cannot be interpreted".format(ds_code))
        f_dsd = zf.open(n_dsd)
        f_data = zf.open(n_data)
        return f_dsd, f_data
    
    
    def _dl_toc(self):
        log.info("Downloading Table of Contents XML file.")
        params = {"file": "table_of_contents.xml"}
        fb = self.download_file(self._base_bulk_url, params, 
                                stream=True, local_repo_file=params["file"])
        parser = etree.XMLParser(remove_blank_text=True)
        self._xml_toc = etree.parse(fb, parser=parser)
        self._xml_toc_nsmap = self._xml_toc.getroot().nsmap
        
        
    def _datasets_to_update(self, latest_update=datetime(1990,1,1), force_undated=True, force_download=True):
        if force_download or not hasattr(self, "_xml_toc"):
            self._dl_toc()
        
        nsmap = self._xml_toc_nsmap
        for ds in self._xml_toc.iterfind('.//nt:leaf[@type="dataset"]', namespaces=nsmap):
            ds_last_update = ds.findtext("nt:lastUpdate", namespaces=nsmap)
            ds_code = ds.findtext("nt:code", namespaces=nsmap)
            try:
                ds_last_update_dt = datetime.strptime(ds_last_update,"%d.%m.%Y")
                if ds_last_update_dt >= latest_update:
                    log.info("Dataset: {0}, last update on {1}.".format(ds_code,ds_last_update))
                    yield ds
            except ValueError:
                #if the data is not visible, yield it if force_undated is true
                if force_undated:
                    log.info("Dataset: {0}, last update not available.".format(ds_code))
                    yield ds
                    
    def update(self, latest_update):
        
        for ds in self._datasets_to_update(latest_update=latest_update, force_undated=False, force_download=True):
            nsmap = self._xml_toc_nsmap
            ds_code = ds.findtext("nt:code", namespaces=nsmap)
            #ds_ancestors = [anc for anc in ds.iterancestors('{*}branch')]
            xml_dsd, xml_data = self._dl_dataset(ds_code)
            dimensions, attributes, t_dimension, pri_measure, concepts, codelists = EurostatFetcher._decompose_dsd(xml_dsd)

