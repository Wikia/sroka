import logging
from functools import lru_cache
from time import time
from typing import Any, Union, Dict, Tuple

from googleads import ad_manager

# yeah i know it's unused, but it's needed for the import to work lol
# pylint: disable=unused-import
import sroka.api.google_ad_manager.gam_api
import sroka
from zeep.helpers import serialize_object
import country_converter as coco

from .exceptions import WeirdGEOException, DataNotFoundException

logger = logging.getLogger(__name__)

PROGRESS_EVERY = 10  # seconds


def statement_iter(service_name: str, statement, object_name: str = "", offset=500):
    """
    Gets objects from a service by statement,
    automatically finds the function and performs pagination.

    It works like an iterator:

    ```python
    for i in statement_iter("OrderService", ...):
        print(i.name)
    ```

    Remember some services have multiple `ByStatement` functions in them and the

    Arguments:
        service_name -- Name of the service to query for the `ByStatement` function
        statement -- PQL statement object

    Keyword Arguments:
        object_name -- specify what is the class of the objects you want to retrieve (default: {""})
        offset -- page size (default: {500})

    Yields:
        Ad manager library objects
    """
    client = sroka.api.google_ad_manager.gam_api.init_gam_connection()

    # Retrieve the appropriate method
    service = client.GetService(service_name)
    method = next(
        (
            getattr(service, method_name)
            for method_name in dir(service.zeep_client.service)
            if method_name.endswith("ByStatement")
            and object_name.lower() in method_name.lower()
        ),
        None,
    )
    if method is None:
        return

    i = 0
    while True:
        paginated_statement = statement.Limit(offset).Offset(i)
        logger.debug("requesting: %s", paginated_statement.ToStatement())
        response = method(paginated_statement.ToStatement())
        if "results" in response and len(response["results"]) > 0:
            i += offset
            yield from response["results"]
        else:
            break


def statement_length(service_name: str, statement, object_name: str = ""):
    """Like `statement_iter`, but returns the number of results. It doesn't use offset."""
    client = sroka.api.google_ad_manager.gam_api.init_gam_connection()

    # Retrieve the appropriate method
    service = client.GetService(service_name)
    method = next(
        (
            getattr(service, method_name)
            for method_name in dir(service.zeep_client.service)
            if method_name.endswith("ByStatement")
            and object_name.lower() in method_name.lower()
        )
    )

    statement_limited = statement.Limit(1)
    logger.debug("requesting: %s", statement_limited.ToStatement())
    response = method(statement_limited.ToStatement())
    return serialize_object(response).get("totalResultSetSize")


def statement_iter_timer(
    service_name: str, statement, object_name: str = "", offset=500
):
    """Like `statement_iter`, but logs tqdm-like progress info."""
    size = statement_length(service_name, statement, object_name)
    start = time()
    last_log = time()
    for ith, i in enumerate(
        statement_iter(service_name, statement, object_name, offset)
    ):
        if size > 0 and (time() - last_log) >= PROGRESS_EVERY:
            last_log = time()
            minutes = round((time() - start) / (ith + 1) * (size - ith))
            logger.info(
                "%.2f percent; remaining time (s): %d", ith * 100 / size, minutes
            )
        yield i


def _custom_fields_to_dict(custom_fields) -> Dict[int, Tuple[Any, bool]]:
    """Converts the list of custom fields to a dict"""
    return {
        i.customFieldId: (i.value.value, False)
        if hasattr(i, "value")
        else (i.customFieldOptionId, True)
        for i in custom_fields
    }


SETUP_CFS_VALUES = {
    1705560: "Not_ready",
    1701289: "Ready",
    1705566: "Checked",
    1705563: "To_recheck",
}
SETUP_CF = 1388408


@lru_cache
def _translate_custom_field_value(val: int) -> Union[str, None]:
    """Translates custom field value ids to readable; result gets cached"""
    client = sroka.api.google_ad_manager.gam_api.init_gam_connection()
    service = client.GetService("CustomFieldService")
    ret = service.getCustomFieldOption(val)
    return ret.displayName if ret is not None else None


@lru_cache
def _translate_custom_field_key(key: Union[str, int]) -> int:
    """Translates custom field ids to str to readable; result gets cached"""
    if isinstance(key, int):
        return key
    statement = (
        ad_manager.StatementBuilder().Where("name = :key").WithBindVariable("key", key)
    )
    return next(statement_iter("CustomFieldService", statement, offset=1)).id


def get_custom_field(
    line_item, field: Union[str, int], translate=True
) -> Union[str, Union[int, None]]:
    """Retrieves the value of a custom field"""
    field_converted = _translate_custom_field_key(field)
    custom_fields = _custom_fields_to_dict(line_item['customFieldValues'])
    try:
        val, needs_parse = custom_fields[field_converted]
    except KeyError:
        return None
    if field_converted == SETUP_CF:
        return SETUP_CFS_VALUES.get(val, None)
    return _translate_custom_field_value(val) if translate and needs_parse else val

def get_custom_fields(line_item, translate_values=True):
    unconverted = _custom_fields_to_dict(line_item['customFieldValues'])
    translated = {}

    for key, (val, needs_parse) in unconverted.items():
        translated_key = _translate_custom_field_key(key)
        translated_val = _translate_custom_field_value(val) if needs_parse and translate_values else val
        translated[translated_key] = translated_val
    return translated

@lru_cache
def get_template_by_id(creative_template_id):
    """Retrieves a creative template"""
    search_statement = (
        ad_manager.StatementBuilder()
        .Where("Id = :id")
        .WithBindVariable("id", creative_template_id)
    )
    try:
        return next(statement_iter("CreativeTemplateService", search_statement))
    except StopIteration as exc:
        raise DataNotFoundException.creative_template(creative_template_id) from exc

def get_template_values(creative):
    if "creativeTemplateVariableValues" not in creative:
        return {}
    return {i['uniqueName']:i['value'] for i in creative['creativeTemplateVariableValues'] if 'value' in i}

def list_video_tracking_urls(creative):
    if "trackingUrls" not in creative:
        return []
    event_urls = []
    for i in creative["trackingUrls"]:
        event = i['key']
        event_urls.extend((event, url) for url in i['value']['urls'])
    return event_urls

_converter = coco.CountryConverter()

def get_geo(targeting):
    if (
        "geoTargeting" not in targeting
        or "targetedLocations" not in targeting["geoTargeting"]
    ):
        return []
    locations = targeting["geoTargeting"]["targetedLocations"]
    location_names = {i["displayName"] for i in locations}
    return _converter.convert(names=location_names, to="ISO3", not_found=None)

def get_device(targeting):
    if (
        "technologyTargeting" not in targeting or
        targeting["technologyTargeting"] is None or
        "deviceCategoryTargeting" not in targeting["technologyTargeting"] or
        "targetedDeviceCategories" not in targeting["technologyTargeting"]["deviceCategoryTargeting"]
    ):
        return []
    return [i['name'] for i in targeting['technologyTargeting']["deviceCategoryTargeting"]["targetedDeviceCategories"]]
