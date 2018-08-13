#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 15 22:01:05 2018

@author: mattange
"""
from sqlalchemy import Column, String
from .BaseModel import BaseModel

class Datasource(BaseModel):
    __tablename__ = "datasources"
    
    id = Column(String, primary_key=True)
    name = Column(String)
    description = Column(String)
    web = Column(String)
    
    def __repr__(self):
        return "<Datasource(id='%s', name='%s')>" % (self.id, self.name)
    