import json

from ratelimit import limits, sleep_and_retry
import requests as requests


def create_market_history_table(conn):
    conn.execute("""
create table  IF NOT EXISTS item_history
(
    itemID INTEGER,
    worldID  INTEGER,
    lastUploadTime  INTEGER,
    entries  TEXT,
    stackSizeHistogram  TEXT,
    stackSizeHistogramNQ  TEXT,
    regularSaleVelocity   INTEGER,
    nqSaleVelocity  INTEGER,
    hqSaleVelocity  INTEGER,
    worldName  TEXT,
    PRIMARY KEY (itemID,worldID,lastUploadTime,worldName)    
)
    """)
    conn.execute("""
    create index IF NOT EXISTS item_history_itemID_index on item_history (itemID desc);
""")
    pass


def __insert_into_item_history__(conn, item_response):
    for i in item_response:
        conn.execute("""
            INSERT INTO item_history SELECT :itemID,:worldID,:lastUploadTime,:entries,:stackSizeHistogram,
                                            :stackSizeHistogramNQ,:regularSaleVelocity,:nqSaleVelocity,:hqSaleVelocity,:worldName
                                    WHERE NOT EXISTS(SELECT 1 FROM item_history WHERE itemID =:itemID AND 
                                            worldID=:worldID AND lastUploadTime=:lastUploadTime and worldName =:worldName)        
            """, {k: (json.dumps(v) if (isinstance(v, list) or isinstance(v, dict)) else v) for k, v in
                  i.items()})


def create_market_table(conn):
    pass


@sleep_and_retry
@limits(calls=6, period=1)
def __get_market_listing(conn, world, items):
    days = 7
    items = items.copy()
    rows = conn.execute(f"""
    select itemID,COUNT(*)
    from item_history
    where itemID in ({','.join([str(i) for i in items])})
      and lower(worldName) = lower('{world}')
      and (lastUploadTime/1000 + {days}*60*60*24) < unixepoch('now') GROUP BY itemID;
    """)

    # Remove any items that already exists in the DB
    items_to_fetch = [str(i[0]) for i in rows]

    if len(items_to_fetch) > 0:
        # Fetch and update
        response = requests.get(
            'https://universalis.app/api/{world}/{items}'.format(world=world, items=','.join(items_to_fetch)),
            headers={'Accept': 'application/json',
                     'user-agent': 'curl/7.74.0'}
        )
        if response.status_code != 200:
            raise ConnectionError(
                f"Got error: {str(response.status_code)} while querying for: {','.join(items_to_fetch)}")
        json_response = response.json()
        if len(items_to_fetch) > 1:
            __insert_into_item_history__(conn, json_response['items'])
            return json_response['unresolvedItems']
        else:
            __insert_into_item_history__(conn, [json_response])
    return []

@sleep_and_retry
@limits(calls=6, period=1)
def __get_market_history(conn, world, items):
    days = 7
    items = items.copy()
    rows = conn.execute(f"""
    select itemID,COUNT(*)
    from item_history
    where itemID in ({','.join([str(i) for i in items])})
      and lower(worldName) = lower('{world}')
      and (lastUploadTime/1000 + {days}*60*60*24) >= unixepoch('now') GROUP BY itemID;
    """)

    # Remove any items that already exists in the DB
    item_present = [i[0] for i in rows]
    items_to_fetch = set(items) - set(item_present)
    misses = []

    if len(items_to_fetch) > 0:
        # Fetch and update
        response = requests.get(
            'https://universalis.app/api/history/{world}/{items}'.format(world=world, items=','.join(
                [str(i) for i in items_to_fetch])),
            headers={'Accept': 'application/json',
                     'user-agent': 'curl/7.74.0'}
        )
        if response.status_code != 200 and response.status_code != 404:
            raise ConnectionError(
                f"Got error: {str(response.status_code)} while querying for: {','.join([str(i) for i in items_to_fetch])}")
        if len(items_to_fetch) > 1:
            json_response = response.json()
            __insert_into_item_history__(conn, json_response['items'])
            misses = json_response['unresolvedItems']
        elif response.status_code == 404:
            misses = [str(i) for i in items_to_fetch]
        else:
            json_response = response.json()
            __insert_into_item_history__(conn, [json_response])
        conn.commit()

    rows = conn.execute(f"""    
    select ih.itemID,
       ih.worldID,
       ih.lastUploadTime,
       ih.entries,
       ih.stackSizeHistogram,
       ih.stackSizeHistogramNQ,
       ih.regularSaleVelocity,
       ih.nqSaleVelocity,
       ih.hqSaleVelocity,
       ih.worldName
    from item_history ih
         INNER JOIN (select itemID,MAX(lastUploadTime) from item_history where item_history.itemID in 
         ({','.join([str(i) for i in items])}) GROUP BY itemID) grouped on grouped.itemID == ih.itemID;
    """)

    return misses, {i[0]: {
        'itemID': i[0],
        'worldID': i[1],
        'lastUploadTime': i[2],
        'entries': json.loads(i[3]),
        'stackSizeHistogram': json.loads(i[4]),
        'stackSizeHistogramNQ': json.loads(i[5]),
        'regularSaleVelocity': i[6],
        'nqSaleVelocity': i[7],
        'hqSaleVelocity': i[8],
        'worldName': i[9]
    } for i in rows}


def __chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def download_market(conn, world='goblin', items=None):
    if items is None:
        items = []
    misses = []
    results = {}
    for items in __chunks([v for v in items], 10):
        _misses, _results = __get_market_listing(conn, world, items)
        misses.append(_misses)
        results.update(_results)
        if len(_misses) > 0:
            print("Unable to get price history for {miss}".format(miss=_misses))

    return misses, results


def download_market_history(conn, world='goblin', items=None, progress=lambda: None):
    if items is None:
        items = []
    misses = []
    results = {}
    for items in __chunks([v for v in items], 10):
        _misses, _results = __get_market_history(conn, world, items)
        progress()
        misses.append(_misses)
        results.update(_results)
        if len(_misses) > 0:
            print("Unable to get price history for {miss}".format(miss=_misses))

    return misses, results
