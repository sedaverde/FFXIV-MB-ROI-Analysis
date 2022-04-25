import os.path

import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import sqlite3 as sq


def load_worlds(dataming_basepath):
    worlds = pd.read_csv(f'{dataming_basepath}/csv/World.csv', header=1)
    worlds = worlds.drop(labels=0, axis=0)
    worlds = worlds.rename(columns={'#': 'WorldID'})
    worlds = worlds.astype({'WorldID': int, 'IsPublic': bool,'Region':int,'UserType':int,'DataCenter':int})
    return worlds


def load_recipes(dataming_basepath):
    recipe = pd.read_csv(f'{dataming_basepath}/csv/Recipe.csv', header=1)
    recipe = recipe.drop(labels=0, axis=0)
    recipe.drop(recipe.columns[[26, 28, 29, 31, 32, 38, 41, 46, 47, 48]], axis=1)
    recipe = recipe.rename(columns={'Number': 'RecipeID', 'RecipeLevelTable': 'Level', 'Item{Result}': 'ItemID',
                                    'Amount{Result}': 'Quant', 'Item{Ingredient}[0]': 'Mat0',
                                    'Item{Ingredient}[1]': 'Mat1',
                                    'Item{Ingredient}[2]': 'Mat2', 'Item{Ingredient}[3]': 'Mat3',
                                    'Item{Ingredient}[4]': 'Mat4',
                                    'Item{Ingredient}[5]': 'Mat5', 'Item{Ingredient}[6]': 'Mat6',
                                    'Item{Ingredient}[7]': 'Mat7',
                                    'Item{Ingredient}[8]': 'Mat8', 'Item{Ingredient}[9]': 'Mat9',
                                    'Amount{Ingredient}[0]': 'Quant0',
                                    'Amount{Ingredient}[1]': 'Quant1', 'Amount{Ingredient}[2]': 'Quant2',
                                    'Amount{Ingredient}[3]': 'Quant3',
                                    'Amount{Ingredient}[4]': 'Quant4', 'Amount{Ingredient}[5]': 'Quant5',
                                    'Amount{Ingredient}[6]': 'Quant6',
                                    'Amount{Ingredient}[7]': 'Quant7', 'Amount{Ingredient}[8]': 'Quant8',
                                    'Amount{Ingredient}[9]': 'Quant9',
                                    'RequiredCraftsmanship': 'Craftsmanship', 'RequiredControl': 'Control',
                                    'QuickSynthCraftsmanship': 'QSCraftsmanship',
                                    'QuickSynthControl': 'QSControl', 'SecretRecipeBook': 'Book', 'CanQuickSynth': 'QS',
                                    'CanHq': 'HQ',
                                    'IsSpecializationRequired': 'Specialized', 'IsExpert': 'Expert',
                                    'Status{Required}': 'StatusRequired',
                                    'Item{Required}': 'ItemRequired'})
    recipe = recipe.astype(dtype=
                           {'RecipeID': 'int', 'CraftType': 'byte', 'Level': 'byte', 'ItemID': 'int', 'Quant': 'byte',
                            'Mat0': 'int',
                            'Mat1': 'int', 'Mat2': 'int', 'Mat3': 'int', 'Mat4': 'int', 'Mat5': 'int', 'Mat6': 'int',
                            'Mat7': 'int',
                            'Mat8': 'int', 'Mat9': 'int', 'Quant0': 'byte', 'Quant1': 'byte', 'Quant2': 'byte',
                            'Quant3': 'byte',
                            'Quant4': 'byte', 'Quant5': 'byte', 'Quant6': 'byte', 'Quant7': 'byte', 'Quant8': 'byte',
                            'Quant9': 'byte',
                            'Craftsmanship': 'uint16', 'Control': 'uint16', 'QSCraftsmanship': 'uint16',
                            'QSControl': 'uint16',
                            'Book': 'byte', 'QualityFactor': 'int', 'StatusRequired': 'int', 'ItemRequired': 'int'})

    recipe = recipe.replace({'True': True, 'False': False})
    item = pd.read_csv(f'{dataming_basepath}/csv/Item.csv', header=1,
                       usecols=['#', 'Description', 'Name', 'StackSize', 'Price{Mid}', 'Price{Low}', 'CanBeHq',
                                'IsDyeable', 'ClassJob{Use}', 'IsUntradable'], low_memory=False)

    item = item.drop(labels=0, axis=0)
    ## Rename # column to ItemID (DB key)
    item = item.rename(
        columns={'#': 'ItemID', 'Price{Mid}': 'PriceMid', 'Price{Low}': 'PriceLow', 'ClassJob{Use}': 'ClassJobUse'})

    item = item.astype(
        {'ItemID': 'int', 'StackSize': 'int', 'Description': 'string', 'Name': 'string', 'PriceLow': 'int',
         'PriceMid': 'int', 'ClassJobUse': 'int'})
    item = item.replace({'True': True, 'False': False})
    item = item.dropna(subset=['Name'])

    return [item, recipe]


def save_table(connection, table_name, data):
    data.to_sql(table_name, connection, if_exists='replace', index=False)  # writes to file
