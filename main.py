import os
import sqlite3

from packaging import version

import roianalysis as roi
import csv

def __chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == '__main__':
    (items, recipes) = roi.load_recipes(os.path.join('.', 'ffxiv-datamining'))

    if version.parse(sqlite3.sqlite_version) < version.parse('3.38.2'):
        raise ValueError(f"Invalid SQLITE Verssion. Need at leat 3.38.2 version, current: {sqlite3.sqlite_version}")

    with sqlite3.connect('data.sqlite', check_same_thread=False) as conn:
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension('libsqlitefunctions')")
        roi.save_table(conn, 'ITEMS', items)
        roi.save_table(conn, 'RECIPES', recipes)
        roi.save_table(conn, 'WORLDS', roi.load_worlds(os.path.join('.', 'ffxiv-datamining')))
        roi.create_market_history_table(conn)
        roi.create_market_listing_table(conn)
        rows = conn.execute('select distinct ItemID from RECIPES WHERE ItemID>0')
        #rows = conn.execute('SELECT r.ItemID FROM RECIPES r JOIN MARKET_LISTINGS ml on ml.itemID = r.ItemID WHERE ml.regularSaleVelocity>0.6  AND length(stackSizeHistogramNQ) > 30 GROUP BY r.ItemID')
        recipes = []
        future_recipes = []
        print("Computing recipes: ", end='')

        for q in __chunks(list(map(lambda x: x[0], rows)), 100):
            recipes.extend(roi.find_recipe(connection=conn, ids_to_query=q))
            print('.', end='', flush=True)

        req = set()
        print(f"\nFound a total of: {len(recipes)} recipes")
        for r in recipes:
            req.update(list(r.required().keys()))
            req.update([r.id])
        print(f"Found {len(req)} ingredients")
        print("Downloading Current Market: ", end='', flush=True)
        current_misses, current_results = roi.download_market(conn, 'goblin', req)
        print(f"\nHad Current {len(current_misses)} misses")
        print(f"Had Current {len(current_results.keys())} success")
        print("Downloading History: ", end='', flush=True)
        historical_misses, historical_results = roi.download_market_history(conn, 'goblin', req)
        print(f"\nHad Historical {len(historical_misses)} misses")
        print(f"Had Historical {len(historical_results.keys())} success")
        with open('data.csv','w',newline='') as f:
            csvwriter=csv.writer(f,dialect='excel', quotechar='"', quoting=csv.QUOTE_ALL)
            for r in recipes:
                r.update_prices(lambda item_id: roi.lookup_market_prices(conn, item_id))
                aaction=r.acquire_action()
                profit=aaction.get_profit()
                profit_percentage=profit/aaction.cost()*100

                if not r.market.action.startswith('V'):
                    csvwriter.writerow([r.name,r.count,r.id,aaction.cost(),r.market.value,aaction.actions(),profit,profit_percentage])
                    print(f"Cost to acquire \"{r.name}\" {r.count} * {r.id}: {aaction.cost()}(MB:{r.market.value}) with: {aaction.actions()} for a profit of: {aaction.get_profit()} or {aaction.get_profit()/aaction.cost()*100}%")

