import os
import json
import sqlite3
from packaging import version

if version.parse(sqlite3.sqlite_version) < version.parse('3.38.2'):
    raise ValueError(f"Invalid SQLITE Verssion. Need at leat 3.38.2 version, current: {sqlite3.sqlite_version}")

with sqlite3.connect('data.sqlite') as conn:
    conn.enable_load_extension(True)
    conn.execute("""
create table  IF NOT EXISTS item_history
(
    itemID TEXT,
    worldID  TEXT,
    lastUploadTime  TEXT,
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
    with open('history.json') as f:
        x = json.load(f)
        conn.execute("""
        INSERT INTO item_history SELECT :itemID,:worldID,:lastUploadTime,:entries,:stackSizeHistogram,
                                        :stackSizeHistogramNQ,:regularSaleVelocity,:nqSaleVelocity,:hqSaleVelocity,:worldName
                                WHERE NOT EXISTS(SELECT 1 FROM item_history WHERE itemID =:itemID AND 
                                        worldID=:worldID AND lastUploadTime=:lastUploadTime and worldName =:worldName)        
        """, {k: (json.dumps(v) if (isinstance(v, list) or isinstance(v, dict)) else v) for k, v in x.items()})
        conn.commit()
        print(f"Version: {sqlite3.sqlite_version}")
        conn.execute("SELECT load_extension('libsqlitefunctions')")
        rows = conn.execute("""select stdev(y.ppu),avg(y.ppu)  from
    (select json_extract(x.value,'$.pricePerUnit') as  ppu, json_extract(x.value,'$.hq') as  hq from item_history,json_each(item_history.entries) x ) y""")
        row = next(rows)
        stddev = row[0]
        avg = row[1]
        rows = conn.execute("""select stdev(y.ppu),avg(y.ppu)  from
    (select json_extract(x.value,'$.pricePerUnit') as  ppu, json_extract(x.value,'$.hq') as  hq from item_history,json_each(item_history.entries) x where abs(ppu - :average) < :doublestdev) y""",
                            {'doublestdev': 2 * stddev, 'average': avg})
        row = next(rows)
        stddev, avg = row[0], row[1]
        print(f"Average: {avg}, Stdev: {stddev}")
        pass
