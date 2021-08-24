"""
Types for our annotations.
"""
from datetime import datetime
from typing import *

class IdentifierRecord(NamedTuple):
    uuid: str
    barcode: str
    generated: datetime
    set_name: str
    set_use: str

class MinimalSampleRecord(NamedTuple):
    id: int
    identifier: str

class SampleRecord(NamedTuple):
    id: int
    identifier: str
    encounter_id: Optional[int]
    type: Optional[str]

class KitRecord(NamedTuple):
    id: int
    identifier: str
    encounter_id: Optional[int]
    rdt_sample_id: Optional[int]
    utm_sample_id: Optional[int]

class OrganismRecord(NamedTuple):
    id: int
    lineage: str

class SequenceReadSetRecord(NamedTuple):
    id: int
    sample_id: int
    urls: Optional[List[str]]

class GenomeRecord(NamedTuple):
    id: int
    sample_id: int
    organism_id: int
    sequence_read_set_id: int

class MinimalLocationRecord(NamedTuple):
    id: int
    identifier: str
