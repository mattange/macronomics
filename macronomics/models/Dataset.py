#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 15 22:01:05 2018

@author: mattange
"""
from datetime import datetime
from sqlalchemy import Column, ForeignKey, String, Date, PrimaryKeyConstraint
from .BaseModel import BaseModel


class Dataset(BaseModel):
    __tablename__ = "datasets"
    __table_args__ = (PrimaryKeyConstraint('datasource_id','id', name='datasource-id'),)
    
    #primary key is composed of 2 columns
    datasource_id = Column(String, ForeignKey('datasources.id'))
    id = Column(String)
    added = Column(Date, default=datetime.date)
    description = Column(String)
    
    def __repr__(self):
        return "<Dataset(id='%s', description='%s')>" % (self.id, self.description)

