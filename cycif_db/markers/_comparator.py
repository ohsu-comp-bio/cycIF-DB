""" Makrer comparator class.
"""
from ..model import Marker


class Marker_Comparator:
    """ A wrapper class for comparison of Marker objects.

    Parameters
    -----------
    marker: Marker object
    fluor_sensitive: bool, default is True.
    anti_sensitive: bool, default is False.
    keep_duplicates: str, default is 'keep'.
    """
    def __init__(self, marker, fluor_sensitive=True, anti_sensitive=False,
                 keep_duplicates='keep') -> None:
        self.marker = marker
        self.fluor_sensitive = fluor_sensitive
        self.anti_sensitive = anti_sensitive
        if keep_duplicates not in ('keep'):
            raise ValueError("Invalid input for argument `keep_duplicates`!")
        self.keep_duplicates = keep_duplicates

    def __repr__(self) -> str:
        rval = self.marker.name
        if self.fluor_sensitive and self.marker.fluor:
            rval += '_' + self.marker.fluor
        if self.anti_sensitive and self.marker.anti:
            rval += '_' + self.marker.anti
        if self.keep_duplicates == 'keep' and self.marker.duplicate:
            rval += '_' + self.marker.duplicate

        return rval

    def __eq__(self, other) -> bool:
        if not isinstance(other, Marker_Comparator):
            return False

        rval = (self.marker.name.lower() == other.marker.name.lower())
        if self.fluor_sensitive:
            this_fluor = self.marker.fluor or ''
            other_fluor = other.marker.fluor or ''
            rval = rval and \
                (this_fluor.lower() == other_fluor.lower())
        if self.anti_sensitive:
            this_anti = self.marker.anti or ''
            other_anti = other.marker.anti or ''
            rval = rval and \
                (this_anti.lower() == other_anti.lower())

        return rval

    def __hash__(self) -> int:
        key = self.marker.name.lower()
        if self.fluor_sensitive and self.marker.fluor:
            key += self.marker.fluor.lower()
        if self.anti_sensitive and self.marker.anti:
            key += self.marker.anti.lower()

        return hash(key)
