# -*- coding: utf-8 -*-
import requests
import logging
from io import BytesIO
from pathlib import Path
from dulwich.repo import Repo

log = logging.getLogger(__name__)

class BaseFetcher():

    def __init__(self, temp_parent_dir=None, repo_parent_dir=None):
        
        if temp_parent_dir is not None:
            self._temp_location = temp_parent_dir + "/" + self.name
            Path(self._temp_location).mkdir(exist_ok=True)
        
        if repo_parent_dir is not None:
            self._repo_location = repo_parent_dir + "/" + self.name
            Path(self._repo_location).mkdir(exist_ok=True)
            try:
                self.repo_load()
            except ValueError:
                self.repo_initialize()
        else:
            self.has_repo = False
            self.repo = None
    

    @property
    def name(self):
        return self._name

    @property
    def repo_location(self):
        return self._repo_location

    def retrieve_metadata(self):
        raise NotImplementedError()
    
    def retrieve_data(self, dataset):
        raise NotImplementedError()
    
    def update_data(self, from_date):
        raise NotImplementedError()

    def download_file(self, url, params, stream=False, local_repo_file=None):
        """
        Downloads a file and returns a BytesIO buffer. 
        Specify if download needs to happen on a stream, default False. 
        If the fetcher has a local repo, the local_repo_file string can be 
        provided to allow storing the file in the repo (relative file path to 
        the repo base path). If no local_repo_file is provided (None), the file 
        is NOT stored in the repo regardless of the fetcher having a repo or not.
        
        """
        log.info("Downloading file from url: {} - params: {}".format(url,params))
        rsp = requests.get(url, params, stream=stream)
        if rsp.status_code == 200:
            fb = BytesIO()
            for chunk in rsp.iter_content(chunk_size=None):
                if chunk:
                    fb.write(chunk)
            fb.seek(0)
            if self.has_repo and local_repo_file is not None:
                log.info("Saving to local file {}".format(local_repo_file))
                ds = local_repo_file.split('/')
                file_name = ds[-1]
                file_sublocation = "/".join(ds[0:-1])
                dir_path = Path(self._repo_location + '/' + file_sublocation)
                dir_path.mkdir(parents=True,exist_ok=True)
                file_path = dir_path.joinpath(file_name)
                with open(file_path, 'wb') as f:
                    f.write(fb.read())
                fb.seek(0)
                self.repo.stage([local_repo_file])
            return fb
        else:
            raise RuntimeError("Downloading the requested file failed wit response status {}.".format(rsp.status_code))    
    
    def repo_initialize(self):
        """
        Initializes a repo on disk where source data should be stored.
        The subdirectory of parent_location is based off the name of the 
        Fetcher. 
        
        """
        pth = Path(self.repo_location)
        gitpth = Path(self.repo_location + '/.git')
        if not pth.is_dir():
            pth.mkdir()
        if gitpth.is_dir():
            raise ValueError("Requested location already contains a repo.")
        self.repo = Repo.init(self.repo_location)
        self.has_repo = True
    
    def repo_load(self):
        """
        Loads an existing repo on disk.
        The repo should be located in the subdirectory of parent_location
        based off the name of the Fetcher.
        
        """
        gitpth = Path(self.repo_location + '/.git')
        if gitpth.is_dir():
            self.repo = Repo(self.repo_location)
            self.has_repo = True
        else:
            raise ValueError("Requested repo does not exist.")

    def repo_commit(self, msg):
        if self.has_repo:
            return self.repo.do_commit(msg.encode('UTF-8'))
        else:
            raise RuntimeError("Repo not initialized / loaded.")
