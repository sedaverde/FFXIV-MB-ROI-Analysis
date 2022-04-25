import os
import sqlite3
import pandas as pd

import numpy

import roianalysis as roi


def __chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == '__main__':
    (items, recipes) = roi.load_recipes(os.path.join('.', 'ffxiv-datamining'))
    with sqlite3.connect('data.sqlite') as conn:
        roi.save_table(conn, 'ITEMS', items)
        roi.save_table(conn, 'RECIPES', recipes)
        roi.save_table(conn, 'WORLDS', roi.load_worlds(os.path.join('.', 'ffxiv-datamining')))
        roi.create_market_history_table(conn)
        rows = conn.execute('select distinct ItemID from RECIPES')
        recipes = []
        print("Computing recipes: ", end='')
        for q in __chunks(list(map(lambda x: x[0], rows)), 20):
            recipes = [*recipes, *(roi.find_recipe(connection=conn, ids_to_query=q))]
            print('.', end='', flush=True)
        req = set()
        for r in recipes:
            req.update(list(r.required().keys()))
        print("\nDownloading History: ", end='', flush=True)
        roi.download_market_history(conn, 'goblin', req, progress=lambda: print('.', end='', flush=True))
