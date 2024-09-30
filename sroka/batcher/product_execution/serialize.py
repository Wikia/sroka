from datetime import datetime
from warnings import warn
from io import StringIO
from string import Template
import yaml
from zeep.helpers import serialize_object
from pytz import timezone
from typing import TextIO
import sroka.api.google_ad_manager.gam_api

from .base import AdProductExecution
from ..gam_helpers import get_template_by_id, get_custom_fields, get_geo, get_template_values, list_video_tracking_urls, get_custom_field, get_device
from ..keyval import Targeting

def as_datetime(dt):
    return datetime(
        year=dt['date']['year'], month=dt['date']['month'], day=dt["date"]["day"],
        hour=dt['hour'], minute=dt['minute'], second=dt['second'],
        tzinfo=timezone(dt['timeZoneId'])
    )

class Serialize:
    def __init__(self, ad_product: AdProductExecution):
        self.ad_product = ad_product

    @staticmethod
    def get_creative_size_type(creative_placeholder):
        if creative_placeholder is None:
            return ""
        elif creative_placeholder['creativeSizeType'] == "NATIVE":
            return get_template_by_id(creative_placeholder["creativeTemplateId"])
        else:
            return creative_placeholder['creativeSizeType']

    @staticmethod
    def get_start_date(line_item):
        if line_item["startDateTimeType"]:
            return as_datetime(line_item["startDateTime"])
        else:
            return line_item["startDateTimeType"]

    @staticmethod
    def get_fcap(line_item):
        fcaps = line_item['frequencyCaps']
        formatted_fcaps = [f"{fc['maxImpressions']}/{fc['numTimeUnits']} {fc['timeUnit'].lower()}" for fc in fcaps]
        if len(formatted_fcaps) == 1:
            return formatted_fcaps[0]
        else:
            return formatted_fcaps

    @staticmethod
    def get_and_format_goal(line_item):
        unit_type = line_item['primaryGoal']['unitType']
        li_type = line_item['lineItemType']
        if li_type == "SPONSORSHIP":
            units = f"{line_item['primaryGoal']['units']}%"
        else:
            units = format(line_item['primaryGoal']['units'], ",")

        return f"{units} {unit_type}"

    @staticmethod
    def get_end_date(line_item):
        return "UNLIMITED" if line_item["unlimitedEndDateTime"] else as_datetime(line_item["endDateTime"])

    def creatives_of_line_item_as_readable(self, line_item) -> list[dict]:
        creatives = self.ad_product.creatives_of(line_item['id'])
        formatted_creatives = []
        for creative in creatives:
            description = {}

            description["Name"] = creative["name"]
            description["ID"] = creative["id"]
            description["Size"] = f"{creative['size']['width']}x{creative['size']['height']}"

            if "creativeTemplateId" in creative:
                description["Creative template"] = get_template_by_id(creative["creativeTemplateId"])['name']
                creative_type_specific = get_template_values(creative)
            elif "vastXmlUrl" in creative:
                creative_type_specific = {
                    "VAST URL": creative["vastXmlUrl"],
                    "Tracking URLs": [f"{i} - {j}" for i,j in list_video_tracking_urls(creative)]
                }
            elif "destinationUrl" in creative:
                creative_type_specific = {
                    "Destinatin URL": creative["destinationUrl"],
                    "Tracking URLs": [f"{i} - {j}" for i,j in list_video_tracking_urls(creative)]
                }
            else:
                mess = f"Creative '{creative['id']}' was not fully rendered due to the lack creative template ID, destination URL or VAST URL"
                warn(mess)
                creative_type_specific = mess

            description["Custom fields"] = get_custom_fields(creative, translate_values=False)
            description["SSL"] = f"{creative['sslScanResult']} {creative['sslManualOverride']}"

            description["Type-specific"] = creative_type_specific
            formatted_creatives.append(description)
        return formatted_creatives

    def as_readable_dict(self) -> dict:
        result = {}
        result["Line Items"] = []

        for line_item in self.ad_product.line_items:
            description = {}
            description["Name"] = line_item["name"]
            description["ID"] = line_item["id"]
            description["Status"] = line_item["status"]
            description["Priority"] = f"{line_item['lineItemType']} {line_item['priority']}"
            description["Expected creatives"] = []

            for expected in line_item["creativePlaceholders"]:
                creative = {}
                creative["Size"] = f"{expected['size']['width']}x{expected['size']['height']}"
                creative["Type"] = self.get_creative_size_type(expected)
                if expected['targetingName']:
                    creative["Targeting"] = expected['targetingName']
                description["Expected creatives"].append(creative)
            description["Expected creatives"].sort(key=lambda x: x["Size"]+x["Type"])

            description["Labels"] = []

            description["Comments"] = line_item['notes']

            description["Custom fields"] = get_custom_fields(line_item, translate_values=False)

            description["Start time"] = self.get_start_date(line_item)
            description["End time"] = self.get_end_date(line_item)
            description["Cost"] = f"{line_item['costPerUnit']['currencyCode']} {int(line_item['costPerUnit']['microAmount'])/(10**6):.2f} {line_item['costType']}"
            description["Goal"] = self.get_and_format_goal(line_item)

            description["Creative rotation"] = line_item['creativeRotationType']
            description['Companions'] = line_item['roadblockingType']
            description["Delivery rate"] = line_item['deliveryRateType']
            description["Forecast source"] = line_item['deliveryForecastSource']
            description["Allow to serve to children"] = line_item['childContentEligibility']
            description['Frequency Cap'] = self.get_fcap(line_item)

            targeting = Targeting.from_results(line_item).in_string.replace('\n'," ")
            description["Custom targeting"] = targeting.replace(" OR ", " OR\n")
            description["Geotargeting"] = get_geo(line_item['targeting'])
            description["Devices"] = ", ".join(get_device(line_item['targeting']))

            creatives = self.creatives_of_line_item_as_readable(line_item)
            creatives.sort(key=lambda x: x["Name"])
            description["Creatives"] = creatives

            result["Line Items"].append(description)
        result["Line Items"].sort(key=lambda x: x["Name"])
        return result

    def yaml_dumps(self) -> list[str]:
        string_io = StringIO()
        self.yaml_dump(string_io)
        string_io.seek(0)
        return string_io.readlines()

    def yaml_dump(self, io: TextIO) -> None:
        yaml.dump(self.as_readable_dict(), io, default_flow_style=False, sort_keys=False)
