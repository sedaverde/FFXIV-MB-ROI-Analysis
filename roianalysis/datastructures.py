import sys
import textwrap
import warnings


class AcquireAction:

    def __init__(self, count: int = 1):
        self._profit = 0
        self._count = count

    def value(self) -> float:
        pass

    def actions(self) -> str:
        pass

    def cost(self) -> float:
        return self._count * self.value()

    def set_profit(self, p: float) -> None:
        self._profit = p

    def get_profit(self) -> float:
        return self._profit


class BuyAction(AcquireAction):

    def __init__(self, value: float, count: int, action: str):
        super().__init__(count)
        self._value = value
        self._action = action

    def value(self) -> float:
        return self._value

    def actions(self) -> str:
        return self._action.format(cost=self.cost())


class CraftAction(AcquireAction):
    def __init__(self, name, item_id, count: int, actions: list[AcquireAction]):
        super().__init__(count)
        self._item_id = item_id
        self._actions = actions
        self._name = name

    def value(self) -> float:
        return self.cost() / self._count

    def cost(self) -> float:
        return sum([i.cost() for i in self._actions])

    def actions(self) -> str:
        a = ','.join([f"({i.actions()})" for i in self._actions])
        return f"C[{self._item_id}!\"{self._name}\"]:{a}"


class MarketItem:

    def __init__(self, id, world, timestamp, value, action):
        self.id = id
        self.world = world
        self.timestamp = timestamp
        self.value = value
        self.action = action
        pass


class Recipe:

    def __init__(self, name, id, count, ingredients):
        self.id = id
        self.name = name
        self.count = int(count)
        self.ingredients = ingredients
        self.market = None
        self.ivalue = None
        self.iaquire = None

    def __repr__(self, indent=0):
        res = """
                Recipe: {id}
                Produces: {count}""".format(id=self.id, count=self.count)
        res = textwrap.dedent(res).lstrip()
        if self.market is not None:
            res += "\nAverage MB Price: {price}".format(price=self.market.value)
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

    def update_prices(self, lookup):
        self.market = None
        entry = lookup(self.id)
        if entry is not None:
            self.market = entry
        else:
            print("Unable to find market value for {id}".format(id=self.id))

        for i in self.ingredients:
            i.update_prices(lookup)
        pass

    def acquire_action(self, perunit=False):
        # How much is it to buy self.count item from the MB
        buy_action = BuyAction(self.market.value, self.count,
                               f"{self.market.action}[{self.id}!\"{self.name}\"@{{cost}}]")
        cactions = []
        for i in self.ingredients:
            cactions.append(i.acquire_action())
        craft_action = CraftAction(self.name, self.id, self.count, cactions)
        # The craft action will return cost of self.count items from recipe
        # To get per-unit value we need to divide by self.count.
        result = buy_action if buy_action.cost() < craft_action.cost() else craft_action
        result.set_profit(buy_action.cost() - craft_action.cost())
        return result


class Ingredient:
    def __init__(self, name, id, count, recipe=None):
        self.id = id
        self.name = name
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

    def update_prices(self, lookup):
        self.market = None
        entry = lookup(self.id)
        if entry is not None:
            self.market = entry
        else:
            print("Unable to find market value for {id}".format(id=self.id))
        if self.hasRecipe():
            self.recipe.update_prices(lookup)
        pass

    def acquire_action(self):
        # Calculate cost of buying vs making
        costToMake = sys.maxsize
        # To craft we would need self.count items from the MB
        buy_action = BuyAction(self.market.value, self.count,
                               f"{self.market.action}[{self.id}\"{self.name}\"@{{cost}}]")

        if not self.hasRecipe():
            return buy_action
        # We want to know how much it would cost PERUNIT to craft the necessary item
        craft_action = self.recipe.acquire_action()

        result = buy_action if buy_action.cost() < craft_action.value() * self.count else craft_action
        result.set_profit(abs(buy_action.value() - (craft_action.value() * self.count)))
        return result


def lookup_market_prices(conn, item_id):
    # Lookup the market price. Priorities are current market listing,
    # historical listings and finally, vendor (with a big fat warning?)
    rows = conn.execute("""
    /*
    This query is not great. Need to fix it to find the lowest Average Price from ALL Worlds
    this is just getting the latest uploaded value... Probably something like MIN(averagePrice) of the
    MAX(lastUploadTime) accross all worlds.
    */
    select ml.itemID, ml.lastUploadTime,ml.worldID,ml.averagePrice FROM  MARKET_LISTINGS ml
         INNER JOIN (select worldID,itemID,MAX(lastUploadTime) as lastUploadTime from MARKET_LISTINGS 
         where MARKET_LISTINGS.itemID in (:itemID) GROUP BY itemID) grouped on grouped.itemID == ml.itemID AND
         grouped.worldID = ml.worldID and grouped.lastUploadTime = ml.lastUploadTime and ml.averagePrice>0
    """, {'itemID': item_id}).fetchall()
    if len(rows) > 1:
        # This can happen, maybe if two worlds get the same upload at the same time?
        warnings.warn(f"Found more than 1 itemID {item_id} that has the latest update.")
        pass

    if len(rows) >= 1:
        r = rows[0]
        if r[0] is not None:
            return MarketItem(item_id, r[2], r[1], r[3], "CB")

    # So not in the current listing.... maybe historical

    rows = conn.execute("""
    with latest_prices as ( SELECT * from (select worldID,itemID,MAX(lastUploadTime) as lastUploadTime from ITEM_HISTORY 
    where ITEM_HISTORY.itemID = :itemID) ih JOIN ITEM_HISTORY_ENTRIES ihe ON ih.itemID = ihe.itemID and 
    ih.worldID = ihe.worldID and ih.lastUploadTime = ihe.lastUploadTime )
    SELECT worldID,lastUploadTime,avg(pricePerUnit) from latest_prices WHERE (ABS(latest_prices.pricePerUnit-(SELECT 
    avg(pricePerUnit) from latest_prices))<3*(SELECT stdev(pricePerUnit) from latest_prices));
    """, {'itemID': item_id}).fetchall()
    if len(rows) > 1:
        warnings.warn(f"Found more than 1 itemid {item_id} that has the latest average historical price")
    if len(rows) >= 1:
        r = rows[0]
        if r[0] is not None:
            return MarketItem(item_id, r[1], r[2], r[3], "HB")

    # Finally might only be available from vendor or old school farming.
    rows = conn.execute("""
    select PriceMid from ITEMS where ItemID = :itemID
    """, {'itemID': item_id}).fetchall()
    r = rows[0]
    return MarketItem(item_id, 0, 0, r[0], "V")
    pass


_name_cache = {}


def _resolve_name(connection, id):
    if id in _name_cache:
        return _name_cache[id]
    row = connection.execute(f"""
    SELECT Name FROM ITEMS WHERE ItemID = {id}
    """)
    result = next(row)[0]
    _name_cache[id] = result
    return result


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
                    Ingredient(_resolve_name(connection, ingredient), ingredient, num_ingredient,
                               subrecipe[0] if len(subrecipe) == 1 else None))
        # Next we append the result to our array
        recipe = Recipe(_resolve_name(connection, row[4]), row[4], row[5], accumulator)
        result.append(recipe)
    return result
