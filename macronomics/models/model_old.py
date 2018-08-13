import pycountry
import pandas as pd

from .features import Frequency, SeasonalAdjustment, SeriesType, SeriesSubType, Theme

class MNSeries(object):

    def __init__(self, identifier, **kwargs):
        self.identifier = identifier
        self.multiplier = 1
        for key in kwargs:
            self[key] = kwargs[key]

    @property
    def identifier(self):
        """ 
        The identifier of the series.
        """
        return self._identifier
    @identifier.setter
    def identifier(self, v):
        self._identifier = v

    @property
    def name(self):
        """
        The descriptive name of the series.
        
        """
        return self._name
    @name.setter
    def name(self, v):
        self._name = v
    

    @property
    def link(self):
        """
        The link to the original datasource, as close as possible to the current series.

        """
        return self._link
    @link.setter
    def link(self, v):
        self._link = v

    @property
    def provider(self):
        """
        The provider code of the original datasource.

        """
        return self._provider
    @provider.setter
    def provider(self, v):
        self._provider = v

    @property
    def provider_identifiers(self):
        """
        The identifiers in the original datasource as dictionary.

        """
        return self._provider_identifiers
    @provider_identifiers.setter
    def provider_identifiers(self, v):
        self._provider_identifiers = v

    @property
    def geography(self):
        """
        The reference geography of the series if available, or None if not meaningful.
        
        """
        return self._geography
    @geography.setter
    def geography(self, v):
        self._geography = v

    @property
    def theme(self):
        """
        The series' theme.

        """
        return self._theme
    @theme.setter
    def theme(self, v):
        if type(v) is not Theme:
            raise TypeError("Theme property need be a Theme object")
        self._theme = v


    @property
    def tags(self):
        """
        The series' default tags.

        """
        return self._tags
    @tags.setter
    def tags(self, v):
        if type(v) is not list:
            raise TypeError("Tags property need be a list object")
        self._tags = v            

    @property
    def frequency(self):
        """
        The series frequency.
                
        """
        return self._frequency
    @frequency.setter
    def frequency(self, v):
        if type(v) is not Frequency:
            raise TypeError("Frequency property need be a Frequency object")
        self._frequency = v            
    
    @property
    def categorical(self):
        """
        Is the series only allowed to take specific values (including binary values)?
        
        """
        return self._categorical
    @categorical.setter
    def categorical(self, v):
        if type(v) is not bool:
            raise TypeError("Categorical property need be a boolean")
        self._categorical = v
        
    @property
    def seasonadj(self):
        """
        Is the series seasonally adjusted? Type of seasonal adjustment.
        
        """
        return self._seasonadj
    @seasonadj.setter
    def seasonadj(self, v):
        if type(v) is not SeasonalAdjustment:
            raise TypeError("Seasonadj property need be a SeasonalAdjustment object")
        self._seasonadj = v
    
    @property
    def series_type(self):
        """
        Define the series' type.
        
        """
        return self._series_type
    @series_type.setter
    def series_type(self, v):
        if type(v) is not SeriesType:
            raise TypeError("Series_type property need be a SeriesType object")
        self._series_type = v
    
    @property
    def series_subtype(self):
        """
        Define the series' subtype.
        
        """
        return self._series_subtype
    @series_subtype.setter
    def series_subtype(self, v):
        if type(v) is not SeriesSubType:
            raise TypeError("Subtype property need be a Subtype object")
        self._series_subtype = v
        
    @property
    def unit(self):
        """
        The series' base unit of measurement;
        for currencies, the 3 letter ISO codes defining the currencies.
        E.g. could be "persons", "kg", "acres", "USD", "EUR", "tonnes", ...
        
        """
        return self._unit
    @unit.setter
    def unit(self, v):
        self._unit = v
        try:
            ccy = pycountry.currencies.get(v)
            self._ccy = ccy
        except KeyError:
            self._ccy = None
         
    
    @property
    def ccy(self):
        """
        The series' currency if it has got one, None otherwise.
        
        """
        return self._ccy
    
    @property
    def multiplier(self):
        """
        Define the series' multiplier of the base unit of measurement.
        
        """
        return self._multiplier
    @multiplier.setter
    def multiplier(self, v):
        self._multiplier = v
    
    @property
    def data(self):
        """
        The underlying data series in Pandas format.
        
        """
        return self._data
    @data.setter
    def data(self, v):
        if type(v) is not pd.Series:
            raise TypeError("Data property need be a Pandas Series object")
            
    