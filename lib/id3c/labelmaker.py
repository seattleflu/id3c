"""
Make physical barcode labels for identifiers.
"""
import logging
import json
import os
import requests
from itertools import chain
from typing import Iterable


LOG = logging.getLogger(__name__)

DEFAULT_LABEL_API = os.environ.get("LABEL_API") \
                 or "https://backoffice.seattleflu.org/labels"


class LabelLayout:
    """
    Layouts, based on the kind of identifier, affect the number of copies of
    each barcode, the label presentation, and label text.
    """
    sku: str
    barcode_type: str
    copies_per_barcode = 1
    reference: str

    blank = {
        "text": "",
        "copies": 1,
    }

    def __init__(self, barcodes):
        if not self.sku:
            raise NotImplementedError("sku must be set by a subclass")

        if not self.barcode_type:
            raise NotImplementedError("barcode_type must be set by a subclass")

        self.barcodes = barcodes

    def label(self, barcode):
        """
        Returns a label spec for the given *barcode*.
        """
        return {
            "text": f"{self.barcode_type} {barcode}\n{self.reference or ''}",
            "barcode": barcode,
            "copies": self.copies_per_barcode,
        }

    def blanks_before(self, barcode_number):
        """
        Returns the number of blank labels to insert before the given
        *barcode_number*.  Defaults to 0 (no blanks).
        """
        return 0

    def spec(self):
        """
        Returns a layout spec suitable for passing to a `Lab Labels
        <https://github.com/MullinsLab/Lab-Labels>`_ web service.
        """
        def flatten(iterable):
            return list(chain.from_iterable(iterable))

        return {
            "type": self.sku,
            "labels": list(
                flatten(
                    (*([self.blank] * self.blanks_before(number)), labels)
                        for number, labels
                         in enumerate(map(self.label, self.barcodes), start = 1)
                )
            ),
        }


class LCRY1100TriplicateLayout(LabelLayout):
    sku = "LCRY-1100"
    copies_per_barcode = 3

    def blanks_before(self, barcode_number):
        """
        Each barcode maps to 3 labels.  Each row is 4 labels wide, so for
        better UX we want all labels in the 4th column to be blank.  We can
        express this without using a mutable label sequence number by inserting
        a blank label before every barcode except the first (e.g. the 2nd
        barcode normally would start filling in the 4th label; by inserting a
        blank, it starts filling in from the 1st label of the next row).
        """
        return 1 if barcode_number > 1 else 0


class SamplesLayout(LabelLayout):
    sku = "LCRY-2380"
    barcode_type = "SAMPLE"
    copies_per_barcode = 3
    reference = "seattleflu.org"

    def blanks_before(self, barcode_number):
        """
        Each barcode maps to 3 labels.  Each row is 7 labels wide, so for
        better UX we want all labels in the 4th column to be blank.  We can
        express this without using a mutable label sequence number by
        inserting a blank label before every other barcode (e.g. the 2nd
        barcode normally would start filling in the 4th label; by inserting a
        blank, it starts filling in from the 5th label).
        """
        return 1 if barcode_number % 2 == 0 else 0


class CollectionsSeattleFluLayout(LabelLayout):
    sku = "LCRY-1100"
    barcode_type = "COLLECTION"
    copies_per_barcode = 1
    reference = "seattleflu.org"


class CollectionsKiosksLayout(LabelLayout):
    sku = "LCRY-1100"
    barcode_type = "KIOSK"
    copies_per_barcode = 2
    reference = "seattleflu.org"


class CollectionsEnvironmentalLayout(LabelLayout):
    sku = "LCRY-1100"
    barcode_type = "ENVIRON"
    copies_per_barcode = 1
    reference = "seattleflu.org"


class CollectionsSwabAndSendLayout(LCRY1100TriplicateLayout):
    barcode_type = "SWAB & SEND"
    reference = "seattleflu.org"


class CollectionsHouseholdObservationLayout(LCRY1100TriplicateLayout):
    barcode_type = "HH OBSERVATION"
    reference = "seattleflu.org"


class CollectionsHouseholdInterventionLayout(LCRY1100TriplicateLayout):
    barcode_type = "HH INTERVENTION"
    reference = "seattleflu.org"


class CollectionsSelfTestLayout(LCRY1100TriplicateLayout):
    barcode_type = "HOME TEST"
    reference = "seattleflu.org"


class CollectionsFluAtHomeLayout(LabelLayout):
    sku = "LCRY-2380"
    barcode_type = "COLLECTION"
    copies_per_barcode = 1
    reference = "fluathome.org"


class KitsFluAtHomeLayout(LabelLayout):
    sku = "LCRY-1100"
    barcode_type = "KIT"
    copies_per_barcode = 1
    reference = "fluathome.org"


class _TestStripsFluAtHomeLayout(LabelLayout):
    sku = "LCRY-2380"
    barcode_type = "TEST STRIP"
    copies_per_barcode = 1
    reference = "fluathome.org"


class CollectionsScanLayout(LabelLayout):
    sku = "LCRY-1100"
    barcode_type = 'SCAN'
    copies_per_barcode = 2
    reference = "scanpublichealth.org"


LAYOUTS = {
    "samples": SamplesLayout,
    "collections-scan": CollectionsScanLayout,
    "collections-seattleflu.org": CollectionsSeattleFluLayout,
    "collections-kiosks": CollectionsKiosksLayout,
    "collections-environmental": CollectionsEnvironmentalLayout,
    "collections-swab&send": CollectionsSwabAndSendLayout,
    "collections-household-observation": CollectionsHouseholdObservationLayout,
    "collections-household-intervention": CollectionsHouseholdInterventionLayout,
    "collections-self-test": CollectionsSelfTestLayout,
    "collections-fluathome.org": CollectionsFluAtHomeLayout,
    "kits-fluathome.org": KitsFluAtHomeLayout,
    "test-strips-fluathome.org": _TestStripsFluAtHomeLayout,
}


def layout_identifiers(set_name: str, identifiers: Iterable) -> LabelLayout:
    """
    Use the layout associated with the given identifier *set_name* to make
    labels for the given *identifiers*.

    Each item in *identifiers* must have a ``barcode`` attribute.  These are
    passed to the layout.
    """
    return LAYOUTS[set_name]([id.barcode for id in identifiers])


def generate_pdf(layout: LabelLayout, api: str = DEFAULT_LABEL_API) -> bytes:
    """
    Generate a PDF from the given *layout* using the `Lab Labels
    <https://github.com/MullinsLab/Lab-Labels>`_ web service *api*.

    Returns a byte string.
    """
    spec = json.dumps(layout.spec())

    LOG.info(f"Generating PDF using Lab Labels API at {api}")

    response = requests.post(f"{api}/stickers",
        headers = { "Content-Type": "application/json" },
        data = spec)

    response.raise_for_status()

    return response.content
