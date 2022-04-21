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
        rows = conn.execute('select distinct ItemID from RECIPES LIMIT 100')
        recipes = []
        for q in __chunks(list(map(lambda x: x[0], rows)), 20):
            recipes = [*recipes, *(roi.find_recipe(connection=conn, ids_to_query=q))]
        for r in recipes:
            print("-------------")
            print(repr(r))

