import os
import sqlite3

import roianalysis as roi

if __name__ == '__main__':
    (items, recipes) = roi.load_recipes(os.path.join('.', 'ffxiv-datamining'))
    with sqlite3.connect('data.sqlite') as conn:
        roi.save_table(conn,'ITEMS', items)
        roi.save_table(conn,'RECIPES', recipes)
        cur=conn.cursor()
        cur.execute('SELECT * FROM ITEMS LIMIT 100')
        rows=cur.fetchall()
        for row in rows:
            print(f"Row=={row}")
