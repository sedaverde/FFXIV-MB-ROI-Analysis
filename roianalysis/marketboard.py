import json

from ratelimit import limits, sleep_and_retry
import requests as requests


def create_market_history_table(conn):
    conn.execute("""
create table  IF NOT EXISTS ITEM_HISTORY
(
    itemID INTEGER,
    worldID  INTEGER,
    lastUploadTime  INTEGER,
    stackSizeHistogram  TEXT,
    stackSizeHistogramNQ  TEXT,
    regularSaleVelocity   INTEGER,
    nqSaleVelocity  INTEGER,
    hqSaleVelocity  INTEGER,
    worldName  TEXT,
    PRIMARY KEY (itemID,worldID,lastUploadTime))
    """)
    conn.execute("""
create table  IF NOT EXISTS ITEM_HISTORY_ENTRIES
(
    itemID INTEGER,
    worldID  INTEGER,
    lastUploadTime  INTEGER,
    idx INTEGER,
    hq INTEGER,
    pricePerUnit INTEGER,
    quantity INTEGER,
    "timestamp" INTEGER,
    PRIMARY KEY (itemID,worldID,lastUploadTime,idx),
    FOREIGN KEY(itemID,worldID,lastUploadTime) REFERENCES ITEM_HISTORY(itemID, worldID, lastUploadTime)
)
    """)
    pass


def create_market_listing_table(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS MARKET_LISTINGS (
  itemID INTEGER,
  worldID INTEGER,  
  lastUploadTime INTEGER,
  currentAveragePrice REAL,
  currentAveragePriceNQ REAL,
  currentAveragePriceHQ REAL,
  regularSaleVelocity REAL,
  nqSaleVelocity REAL,
  hqSaleVelocity REAL,
  averagePrice REAL,
  averagePriceNQ REAL,
  averagePriceHQ REAL,
  minPrice INTEGER,
  minPriceNQ INTEGER,
  minPriceHQ INTEGER,
  maxPrice INTEGER,
  maxPriceNQ INTEGER,
  maxPriceHQ INTEGER,
  stackSizeHistogram TEXT,
  stackSizeHistogramNQ TEXT,
  stackSizeHistogramHQ TEXT,
  worldName TEXT,
  PRIMARY KEY (itemID,worldID,lastUploadTime)
  )
""")
    conn.execute("create index IF NOT EXISTS market_listing_itemID_index on MARKET_LISTINGS (itemID desc);")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS MARKET_LISTINGS_CURRENT (
      itemID INTEGER,
      worldID INTEGER,  
      lastUploadTime INTEGER,
      idx INTEGER,
      lastReviewTime INTEGER,
      pricePerUnit INTEGER,
      quantity INTEGER,
      stainID INTEGER,
      creatorName TEXT,
      creatorID TEXT,
      hq INTEGER,
      isCrafted INTEGER,
      listingID TEXT,
      materia TEXT,
      onMannequin INTEGER,
      retainerCity INTEGER,
      retainerID TEXT,
      retainerName TEXT,
      sellerID TEXT,
      total INTEGER,
      PRIMARY KEY (itemID,worldID,lastUploadTime,idx),
      FOREIGN KEY(itemID,worldID,lastUploadTime) REFERENCES MARKET_LISTINGS(itemID, worldID, lastUploadTime)
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS MARKET_LISTINGS_RECENT_HISTORY (
      itemID INTEGER,
      worldID INTEGER,  
      lastUploadTime INTEGER,
      idx INTEGER,
      hq INTEGER,
      pricePerUnit INTEGER,
      quantity INTEGER,
      timestamp INTEGER,
      buyerName TEXT,
      total INTEGER,      
      PRIMARY KEY (itemID,worldID,lastUploadTime,idx),
      FOREIGN KEY(itemID,worldID,lastUploadTime) REFERENCES MARKET_LISTINGS(itemID, worldID, lastUploadTime)
    )
    """)
    pass


def __insert_into_market_listing__(conn, item_response):
    for i in item_response:
        i.update({'worldID': '8',
                  'worldName': 'Goblin'
                  })
        conn.execute("""
            INSERT INTO MARKET_LISTINGS SELECT :itemID,:worldID,:lastUploadTime,:currentAveragePrice,
            :currentAveragePriceNQ,  :currentAveragePriceHQ,  :regularSaleVelocity,  :nqSaleVelocity,
            :hqSaleVelocity,  :averagePrice,  :averagePriceNQ,  :averagePriceHQ,  :minPrice,  :minPriceNQ,  :minPriceHQ,
            :maxPrice,  :maxPriceNQ,  :maxPriceHQ,  :stackSizeHistogram,  :stackSizeHistogramNQ,  :stackSizeHistogramHQ,
            :worldName
                                    WHERE NOT EXISTS(SELECT 1 from MARKET_LISTINGS WHERE itemID =:itemID AND 
                                            worldID=:worldID AND lastUploadTime=:lastUploadTime)        
            """, {k: (json.dumps(v) if (isinstance(v, list) or isinstance(v, dict)) else v) for k, v in
                  filter(lambda t: t[0] not in ('listings', 'recentHistory'), i.items())})

        pk = {key: i[key] for key in ['itemID', 'worldID', 'lastUploadTime']}
        for idx, e in enumerate(i['listings']):
            pk.update({k: (json.dumps(v) if (isinstance(v, list) or isinstance(v, dict)) else v) for k, v in e.items()})
            pk.update({'idx': idx})
            conn.execute("""
                INSERT INTO MARKET_LISTINGS_CURRENT SELECT :itemID,:worldID, :lastUploadTime, :idx, :lastReviewTime,
                :pricePerUnit, :quantity, :stainID, :creatorName, :creatorID, :hq, :isCrafted, :listingID, :materia,
                :onMannequin, :retainerCity, :retainerID, :retainerName, :sellerID, :total
                WHERE NOT EXISTS(SELECT 1 from MARKET_LISTINGS_CURRENT WHERE itemID =:itemID AND 
                                            worldID=:worldID AND lastUploadTime=:lastUploadTime AND idx=:idx) 
                """, pk)

        pk = {key: i[key] for key in ['itemID', 'worldID', 'lastUploadTime']}
        for idx, e in enumerate(i['recentHistory']):
            pk.update({k: (json.dumps(v) if (isinstance(v, list) or isinstance(v, dict)) else v) for k, v in e.items()})
            pk.update({'idx': idx})
            conn.execute("""
                INSERT INTO MARKET_LISTINGS_RECENT_HISTORY SELECT :itemID,:worldID, :lastUploadTime, :idx, :hq, 
                :pricePerUnit, :quantity, :timestamp, :buyerName, :total
                WHERE NOT EXISTS(SELECT 1 from MARKET_LISTINGS_RECENT_HISTORY WHERE itemID =:itemID AND 
                                            worldID=:worldID AND lastUploadTime=:lastUploadTime AND idx=:idx) 
                """, pk)
    pass


def __insert_into_item_history__(conn, item_response):
    for i in item_response:
        conn.execute("""
            INSERT INTO ITEM_HISTORY SELECT :itemID,:worldID,:lastUploadTime,:stackSizeHistogram,
                                            :stackSizeHistogramNQ,:regularSaleVelocity,:nqSaleVelocity,:hqSaleVelocity,:worldName
                                    WHERE NOT EXISTS(SELECT 1 from ITEM_HISTORY WHERE itemID =:itemID AND 
                                            worldID=:worldID AND lastUploadTime=:lastUploadTime)        
            """, {k: (json.dumps(v) if (isinstance(v, list) or isinstance(v, dict)) else v) for k, v in
                  filter(lambda t: t[0] != 'entries', i.items())})

        pk = {key: i[key] for key in ['itemID', 'worldID', 'lastUploadTime']}
        for idx, e in enumerate(i['entries']):
            pk.update({k: (json.dumps(v) if (isinstance(v, list) or isinstance(v, dict)) else v) for k, v in e.items()})
            pk.update({'idx': idx})
            conn.execute("""
                INSERT INTO ITEM_HISTORY_ENTRIES SELECT :itemID,:worldID, :lastUploadTime, :idx, :hq, :pricePerUnit, :quantity, :timestamp
                WHERE NOT EXISTS(SELECT 1 from ITEM_HISTORY_ENTRIES WHERE itemID =:itemID AND 
                                            worldID=:worldID AND lastUploadTime=:lastUploadTime AND idx=:idx) 
                """, pk)


@sleep_and_retry
@limits(calls=6, period=1)
def __get_market_listing(conn, world, items):
    days = 7
    items = items.copy()
    rows = conn.execute(f"""
    select itemID,COUNT(*)
    from MARKET_LISTINGS
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
            'https://universalis.app/api/{world}/{items}'.format(world=world,
                                                                 items=','.join([str(i) for i in items_to_fetch])),
            headers={'Accept': 'application/json',
                     'user-agent': 'curl/7.74.0'}
        )
        if response.status_code != 200 and response.status_code != 404:
            raise ConnectionError(
                f"Got error: {str(response.status_code)} while querying for: {','.join([str(i) for i in items_to_fetch])}")
        json_response = response.json()
        if len(items_to_fetch) > 1:
            __insert_into_market_listing__(conn, json_response['items'])
            misses.extend(json_response['unresolvedItems'])
        elif response.status_code == 404:
            misses.extend([str(i) for i in items_to_fetch])
        else:
            json_response = response.json()
            __insert_into_market_listing__(conn, [json_response])
        conn.commit()

    rows = conn.execute(f"""    
    select ml.itemID,
        ml.worldID,
        ml.lastUploadTime,
        ml.currentAveragePrice,
        ml.currentAveragePriceNQ,
        ml.currentAveragePriceHQ,
        ml.regularSaleVelocity,
        ml.nqSaleVelocity,
        ml.hqSaleVelocity,
        ml.averagePrice,
        ml.averagePriceNQ,
        ml.averagePriceHQ,
        ml.minPrice,
        ml.minPriceNQ,
        ml.minPriceHQ,
        ml.maxPrice,
        ml.maxPriceNQ,
        ml.maxPriceHQ,
        ml.stackSizeHistogram,
        ml.stackSizeHistogramNQ,
        ml.stackSizeHistogramHQ,
        ml.worldName
    from MARKET_LISTINGS ml
         INNER JOIN (select itemID,MAX(lastUploadTime) from MARKET_LISTINGS where MARKET_LISTINGS.itemID in 
         ({','.join([str(i) for i in items])}) GROUP BY itemID) grouped on grouped.itemID == ml.itemID;
    """)

    return misses, {i[0]: {
        'itemID': i[0],
        'worldID': i[1],
        'lastUploadTime': i[2],
        'currentAveragePrice': i[3],
        'currentAveragePriceNQ': i[4],
        'currentAveragePriceHQ': i[5],
        'regularSaleVelocity': i[6],
        'nqSaleVelocity': i[7],
        'hqSaleVelocity': i[8],
        'averagePrice': i[9],
        'averagePriceNQ': i[10],
        'averagePriceHQ': i[11],
        'minPrice': i[12],
        'minPriceNQ': i[13],
        'minPriceHQ': i[14],
        'maxPrice': i[15],
        'maxPriceNQ': i[16],
        'maxPriceHQ': i[17],
        'stackSizeHistogram': json.loads(i[18]),
        'stackSizeHistogramNQ': json.loads(i[19]),
        'stackSizeHistogramHQ': json.loads(i[20]),
        'worldName': i[21]
    } for i in rows}


@sleep_and_retry
@limits(calls=6, period=1)
def __get_market_history(conn, world, items):
    days = 7
    items = items.copy()
    rows = conn.execute(f"""
    select itemID,COUNT(*)
    from ITEM_HISTORY
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
            misses.extend(json_response['unresolvedItems'])
        elif response.status_code == 404:
            misses.extend([str(i) for i in items_to_fetch])
        else:
            json_response = response.json()
            __insert_into_item_history__(conn, [json_response])
        conn.commit()

    rows = conn.execute(f"""    
    select ih.itemID,
       ih.worldID,
       ih.lastUploadTime,
       ih.stackSizeHistogram,
       ih.stackSizeHistogramNQ,
       ih.regularSaleVelocity,
       ih.nqSaleVelocity,
       ih.hqSaleVelocity,
       ih.worldName
    from ITEM_HISTORY ih
         INNER JOIN (select itemID,MAX(lastUploadTime) from ITEM_HISTORY where ITEM_HISTORY.itemID in 
         ({','.join([str(i) for i in items])}) GROUP BY itemID) grouped on grouped.itemID == ih.itemID;
    """)

    return misses, {i[0]: {
        'itemID': i[0],
        'worldID': i[1],
        'lastUploadTime': i[2],
        'stackSizeHistogram': json.loads(i[3]),
        'stackSizeHistogramNQ': json.loads(i[4]),
        'regularSaleVelocity': i[5],
        'nqSaleVelocity': i[6],
        'hqSaleVelocity': i[7],
        'worldName': i[8]
    } for i in rows}


def __chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def download_market(conn, world='goblin', items=None, progress=lambda: print('.', end='', flush=True)):
    if items is None:
        items = []
    misses = []
    results = {}
    for items in __chunks([v for v in items], 10):
        _misses, _results = __get_market_listing(conn, world, items)
        progress()
        misses.extend(_misses)
        results.update(_results)
        if len(_misses) > 0:
            print("Unable to get price history for {miss}".format(miss=_misses))

    return misses, results


def download_market_history(conn, world='goblin', items=None, progress=lambda: print('.', end='', flush=True)):
    if items is None:
        items = []
    misses = []
    results = {}
    for items in __chunks([v for v in items], 10):
        _misses, _results = __get_market_history(conn, world, items)
        progress()
        misses.extend(_misses)
        results.update(_results)
        if len(_misses) > 0:
            print("Unable to get price history for {miss}".format(miss=_misses))

    return misses, results
