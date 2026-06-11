from configparser import NoOptionError

import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

import sroka.config.config as config

GAM_REST_BASE_URL = 'https://admanager.googleapis.com/v1'
GAM_REST_SCOPE = 'https://www.googleapis.com/auth/admanager'
GAM_REST_DEFAULT_PAGE_SIZE = 1000

KEY_FILE = config.get_file_path('google_ad_manager')


def init_gam_rest_connection(network_code=None):
    """
    Creates an authenticated session for the GAM REST (Beta) API.

    Reuses the same service account key file as the SOAP connection
    (ad_manager.json) and the same network_code from config.ini.

    Args:
        network_code (int|str): GAM network code. Falls back to config.ini.

    Returns:
        tuple: (AuthorizedSession, network_code str), or (None, None) on error.
    """
    if not network_code:
        try:
            network_code = config.get_value('google_ad_manager', 'network_code')
        except (KeyError, NoOptionError):
            print('No network code was provided')
            return None, None

    credentials = service_account.Credentials.from_service_account_file(
        KEY_FILE,
        scopes=[GAM_REST_SCOPE],
    )
    session = AuthorizedSession(credentials)
    return session, str(network_code)


def get_resource_from_admanager(
    resource: str,
    filter_str: str = None,
    page_size: int = None,
    order_by: str = None,
    network_code=None,
) -> pd.DataFrame:
    """
    Fetches a complete list of a specified resource from Google Ad Manager
    via the REST (Beta) API.

    Handles pagination automatically using nextPageToken to retrieve all
    entities matching the query.

    Args:
        resource (str): The resource type to fetch. Must be a key in the
            resource_map (e.g. 'PrivateAuction', 'PrivateAuctionDeal').
        filter_str (str): Optional filter expression in GAM REST filter syntax,
            e.g. "displayName = 'My Auction'".
        page_size (int): Items per page (max 1000, default 1000).
        order_by (str): Optional ordering expression, e.g. 'displayName ASC'.
        network_code (int|str): GAM network code. Falls back to config.ini.

    Returns:
        pd.DataFrame: All items across all pages, one row per item. Returns
            an empty DataFrame on auth failure or a failed request.

    Raises:
        ValueError: If the provided resource is not supported.
    """
    resource_map = {
        'PrivateAuction': 'privateAuctions',
        'PrivateAuctionDeal': 'privateAuctionDeals',
    }

    if resource not in resource_map:
        raise ValueError(
            f"Unsupported resource: '{resource}'. "
            f"Supported resources are: {list(resource_map.keys())}"
        )

    resource_path = resource_map[resource]
    print(f"Initializing REST connection to fetch '{resource}' entities...")

    session, network_code = init_gam_rest_connection(network_code)
    if session is None:
        return pd.DataFrame()

    url = f'{GAM_REST_BASE_URL}/networks/{network_code}/{resource_path}'
    params = {'pageSize': page_size or GAM_REST_DEFAULT_PAGE_SIZE}
    if filter_str:
        params['filter'] = filter_str
    if order_by:
        params['orderBy'] = order_by

    all_items = []
    page_number = 1

    while True:
        print(f'Fetching page {page_number} (pageSize: {params["pageSize"]})...')
        response = session.get(url, params=params)

        if not response.ok:
            print(f'Request failed ({response.status_code}): {response.text}')
            return pd.DataFrame()

        data = response.json()
        items = data.get(resource_path, [])
        num_results = len(items)
        print(f'-> Found {num_results} items on this page.')

        all_items.extend(items)

        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            break

        params['pageToken'] = next_page_token
        page_number += 1

    print(f"Successfully fetched a total of {len(all_items)} '{resource}' items.\n")
    return pd.DataFrame(all_items)


def get_private_auctions_from_admanager(
    filter_str: str = None,
    page_size: int = None,
    order_by: str = None,
    network_code=None,
) -> pd.DataFrame:
    """
    Fetches all Private Auctions from Google Ad Manager via the REST (Beta) API.

    Reference:
        https://developers.google.com/ad-manager/api/beta/reference/rest/v1/networks.privateAuctions

    Args:
        filter_str (str): Optional filter expression,
            e.g. "displayName = 'My Auction'".
        page_size (int): Items per page (max 1000, default 1000).
        order_by (str): Optional ordering, e.g. 'displayName ASC'.
        network_code (int|str): GAM network code. Falls back to config.ini.

    Returns:
        pd.DataFrame: One row per private auction.
    """
    return get_resource_from_admanager(
        resource='PrivateAuction',
        filter_str=filter_str,
        page_size=page_size,
        order_by=order_by,
        network_code=network_code,
    )


def get_private_auction_deals_from_admanager(
    filter_str: str = None,
    page_size: int = None,
    order_by: str = None,
    network_code=None,
) -> pd.DataFrame:
    """
    Fetches all Private Auction Deals from Google Ad Manager via the REST (Beta) API.

    Reference:
        https://developers.google.com/ad-manager/api/beta/reference/rest/v1/networks.privateAuctionDeals

    Args:
        filter_str (str): Optional filter expression,
            e.g. "status = 'ACTIVE'".
        page_size (int): Items per page (max 1000, default 1000).
        order_by (str): Optional ordering, e.g. 'createTime DESC'.
        network_code (int|str): GAM network code. Falls back to config.ini.

    Returns:
        pd.DataFrame: One row per private auction deal.
    """
    return get_resource_from_admanager(
        resource='PrivateAuctionDeal',
        filter_str=filter_str,
        page_size=page_size,
        order_by=order_by,
        network_code=network_code,
    )
