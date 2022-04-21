import sys
import textwrap


class MarketItem:

    def __init__(self, id, world, data):
        self.id = id
        self.world = world
        self.data = data
        pass


class Recipe:

    def __init__(self, id, count, ingredients):
        self.id = id
        self.count = int(count)
        self.ingredients = ingredients
        self.market = None
        self.lookup = {}

    def __repr__(self, indent=0):
        res = """
                Recipe: {id}
                Produces: {count}""".format(id=self.id, count=self.count)
        res = textwrap.dedent(res).lstrip()
        if self.market is not None:
            res += "\nAverage Price: {price}".format(price=self.market.data['averagePrice'])
        res += '\nIngredients'
        res += "".join(map(lambda x: "\n" + x.__repr__(indent + 2), self.ingredients))
        return textwrap.indent(res, ' ' * indent)

    '''
    Will return what is required to craft this recipe.
    
    acc : an accumulator that will provide a place to store the result
    depth: how deep do we want to buy things. default if buy all the leafs
    '''

    def required(self, acc=None, depth=sys.maxsize):
        if acc is None:
            acc = {}
        for i in self.ingredients:
            i.required(acc, depth)
        return acc

    ''' 
        Given a set of ingredients, what would be left
        After crafting this recipe.
        
        acc: What we start with
        depth: how deep do we want to go (buy vs craft) default we craft all                
    '''

    def craft(self, acc=None, depth=sys.maxsize):
        if acc is None:
            acc = {}
        for i in self.ingredients:
            i.craft(acc, depth)
        if self.id not in acc:
            acc[self.id] = 0
        acc[self.id] = acc[self.id] + self.count
        return acc

    def _update_prices_(self, lookup):
        self.market = None
        if self.id in lookup:
            entry = lookup[self.id]
            self.market = MarketItem(world=entry['worldID'], id=self.id, data=entry)
        else:
            print("Unable to find market value for {id}".format(id=self.id))
        for i in self.ingredients:
            i._update_prices_(lookup)
        pass


class Ingredient:
    def __init__(self, id, count, recipe=None):
        self.id = id
        self.count = int(count)
        self.recipe = recipe
        self.market = None
        pass

    def hasRecipe(self):
        return self.recipe is not None

    def __repr__(self, indent=0):
        res = """
                - Ingredient: {id}
                - Required: {count}""".format(id=self.id, count=self.count)
        res = textwrap.dedent(res).lstrip()
        if self.market is not None:
            res += "\nAverage Price: {price}".format(price=self.market.data['averagePrice'])
        if self.recipe is not None:
            res += "\n" + self.recipe.__repr__(indent + 2)
        return textwrap.indent(res, ' ' * indent)

    def required(self, acc, depth):
        if self.id not in acc:
            acc[self.id] = 0
        if self.hasRecipe() and depth > 0:
            self.recipe.required(acc, depth - 1)
        else:
            acc[self.id] = acc[self.id] + self.count
        pass

    def craft(self, acc, depth):
        if self.id not in acc:
            acc[self.id] = 0
        if acc[self.id] < self.count and self.hasRecipe() and depth > 0:
            self.recipe.craft(acc, depth - 1)
        acc[self.id] -= self.count

    def _update_prices_(self, lookup):
        self.market = None
        if self.id in lookup:
            entry = lookup[self.id]
            self.market = MarketItem(world=entry['worldID'], id=self.id, data=entry)
        else:
            print("Unable to find market value for {id}".format(id=self.id))
        if self.hasRecipe():
            self.recipe._update_prices_(lookup)
        pass


def find_recipe(connection, ids_to_query):
    in_ids_to_query = ",".join([str(x) if type(x) == int else x for x in ids_to_query])
    select_stmt = """SELECT * FROM (
                SELECT *,ROW_NUMBER() OVER (PARTITION BY R."ItemID" ORDER BY R."ItemID" DESC) ROWN
                FROM
                RECIPES R WHERE R."ItemID" in ({0}) ) WHERE ROWN = 1
            """.format(in_ids_to_query)
    # Run the query to get the root recipes
    rows = connection.execute(select_stmt)
    # Somewhere to store the result
    result = []
    # Next for each item recipe we get a row
    for row in rows:
        # Where we accumulate the list of ingredients
        accumulator = []
        # Where in the DB result the ingredients are located
        # starts are column 6 (zero index) and there are max 10 ingredients, each ingredient having item+number
        # So 6+10*2==26
        ingredient_list = list(row)[6:26]
        # Create an index we can use to track our position in the array
        for index in range(0, 20, 2):
            # Ingredients[n]
            ingredient = ingredient_list[index]
            # IngredientsCount[n]
            num_ingredient = ingredient_list[index + 1]
            # If the ingredient is 0, just ignore it.
            if int(ingredient) > 0:
                # try and find if the ingredient has a recipe
                subrecipe = find_recipe(connection, [ingredient])
                # For a given ingredient we see if there is a recipe attached to creating this
                # ingredient.
                accumulator.append(
                    Ingredient(ingredient, num_ingredient, subrecipe[0] if len(subrecipe) == 1 else None))
        # Next we append the result to our array
        result.append(Recipe(row[4], row[5], accumulator))
    return result
