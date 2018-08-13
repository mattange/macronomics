from enum import Enum

class Frequency(Enum):
    A = "A"
    Annually = "A"
    Q = "Q"
    Quarterly = "Q"
    S = "H"
    SemiAnnually = "H"
    H = "H"
    HalfYearly = "H"
    M = "M"
    Monthly = "M"
    W = "W"
    Weekly = "W"
    D = "D"
    Daily = "D"

class SeasonalAdjustment(Enum):
    NSA = "nsa"
    SA = "sa"

class SeriesType(Enum):
    STOCK = "stock"
    FLOW = "flow"
    UNKNOWN = "unknown"

class SeriesSubType(Enum):
    SPOT = "spot"
    AVG = "average"
    CHG = "change"
    UNKNOWN = "unknown"
    
class Theme(Enum):
    GENSTAT = "General and regional statistics"
    ECOFIN = "Economy and finance"
    POPUL = "Population and social conditions"
    INDSVCS = "Industry, trade and services"
    AGR = "Agriculture and fisheries"
    INTTRA = "International trade in goods and services"
    TRANSPORT = "Transport"
    ENV = "Environment and energy"
    SCI = "Science, technology, digital society"
    OTHER = "Other"
    
class SubTheme(Enum):
    OTHER = "Other"
