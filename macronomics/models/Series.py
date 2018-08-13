#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 15 22:01:05 2018

@author: mattange
"""
#from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import Column, String, Enum, ForeignKeyConstraint, Boolean
from sqlalchemy.types import ARRAY

from .BaseModel import BaseModel

from .features import Frequency, SeasonalAdjustment, SeriesType, SeriesSubType, Theme, SubTheme

class BaseSeries():
    __table_args__ = (
            ForeignKeyConstraint(
                    ['datasource_id','dataset_id'], 
                    ['datasets.datasource_id', 'datasets.id']
                    ),
            )
    
    id = Column(String, primary_key=True)
    dataset_id = Column(String)
    datasource_id = Column(String)
    name = Column(String)
    
    theme = Column(Enum(Theme))
    subtheme = Column(Enum(SubTheme))
    
    tags = Column(ARRAY(String))
    
    frequency = Column(Enum(Frequency))
    seasonal_adjustment = Column(Enum(SeasonalAdjustment))
    series_type = Column(Enum(SeriesType))
    series_subtype = Column(Enum(SeriesSubType))
    categorical = Column(Boolean, default=False)
    
    def __repr__(self):
        return "<Series(id='%s', dataset='%s', datasource='%s')>" % (self.id, self.dataset_id, self.datasource_id)


def SeriesFactory(name):
    return type(name, (BaseSeries, BaseModel), {'__tablename__': name})

