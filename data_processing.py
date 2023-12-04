import csv, os

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, "Cities.csv")) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, "Countries.csv")) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None


import copy


class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table

    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + "_joins_" + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table

    def filter(self, condition):
        filtered_table = Table(self.table_name + "_filtered", [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)

    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ":" + str(self.table)


# table1 = Table("cities", cities)
# table2 = Table("countries", countries)
# my_DB = DB()
# my_DB.insert(table1)
# my_DB.insert(table2)
# my_table1 = my_DB.search("cities")

# print("Test filter: only filtering out cities in Italy")
# my_table1_filtered = my_table1.filter(lambda x: x["country"] == "Italy")
# print(my_table1_filtered)
# print()

# print("Test select: only displaying two fields, city and latitude, for cities in Italy")
# my_table1_selected = my_table1_filtered.select(["city", "latitude"])
# print(my_table1_selected)
# print()

# print("Calculting the average temperature without using aggregate for cities in Italy")
# temps = []
# for item in my_table1_filtered.table:
#     temps.append(float(item["temperature"]))
# print(sum(temps) / len(temps))
# print()

# print("Calculting the average temperature using aggregate for cities in Italy")
# print(my_table1_filtered.aggregate(lambda x: sum(x) / len(x), "temperature"))
# print()

# print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
# my_table2 = my_DB.search("countries")
# my_table3 = my_table1.join(my_table2, "country")
# my_table3_filtered = my_table3.filter(lambda x: x["EU"] == "no").filter(
#     lambda x: float(x["temperature"]) < 5.0
# )
# print(my_table3_filtered.table)
# print()
# print("Selecting just three fields, city, country, and temperature")
# print(my_table3_filtered.select(["city", "country", "temperature"]))
# print()

# print("Print the min and max temperatures for cities in EU that do not have coastlines")
# my_table3_filtered = my_table3.filter(lambda x: x["EU"] == "yes").filter(
#     lambda x: x["coastline"] == "no"
# )
# print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), "temperature"))
# print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), "temperature"))
# print()

# print("Print the min and max latitude for cities in every country")
# for item in my_table2.table:
#     my_table1_filtered = my_table1.filter(lambda x: x["country"] == item["country"])
#     if len(my_table1_filtered.table) >= 1:
#         print(
#             item["country"],
#             my_table1_filtered.aggregate(lambda x: min(x), "latitude"),
#             my_table1_filtered.aggregate(lambda x: max(x), "latitude"),
#         )
# print()

players = []
with open(os.path.join(__location__, "Players.csv")) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, "Teams.csv")) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))


# table_player = Table("players", players)
# table_team = Table("teams", teams)
# my_DB = DB()
# my_DB.insert(table_player)
# my_DB.insert(table_team)
# my_player = my_DB.search("players")
# my_team = my_DB.search("teams")

# print("Filter Player Task 1.1")
# my_player_filtered = my_player.filter(lambda x: "ia" in x["team"])
# my_player_filtered.filter(lambda x: int(x["minutes"]) < 200 and int(x["passes"]) > 100)
# for i in my_player_filtered.table:
#     print("{} {} {}".format(i["surname"], i["team"], i["position"]))

# print("Filter Player Task 1.2")
# my_team_filtered_rank_below_10 = my_team.filter(lambda x: int(x["ranking"]) < 10)
# my_team_filtered_rank_above_10 = my_team.filter(lambda x: int(x["ranking"]) >= 10)
# team_below_ten = [int(x["games"]) for x in my_team_filtered_rank_below_10.table]
# team_above_ten = [int(x["games"]) for x in my_team_filtered_rank_above_10.table]
# print("The Average of game below 10", sum(team_below_ten) / len(team_below_ten))
# print("The Average of game above 10", sum(team_above_ten) / len(team_above_ten))

# print("Filter Player Task 1.3")
# my_player_filtered_midfielder = my_player.filter(
#     lambda x: x["position"] == "midfielder"
# )
# my_player_filtered_forward = my_player.filter(lambda x: x["position"] == "forward")

# midfielders = [int(x["passes"]) for x in my_player_filtered_midfielder.table]
# forwards = [int(x["passes"]) for x in my_player_filtered_forward.table]

# print("The Average of Midfielder", sum(midfielders) / len(midfielders))
# print("The Average of Forward", sum(forwards) / len(forwards))

titanics = []
with open(os.path.join(__location__, "Titanic.csv")) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanics.append(dict(r))

table_titanic = Table("titanics", titanics)
my_DB = DB()
my_DB.insert(table_titanic)
my_titanic = my_DB.search("titanics")

print("Filter Titanic Task 1.4")
my_titanic_filtered_first_class = my_titanic.filter(lambda x: int(x["class"]) == 1)
my_titanic_filtered_third_class = my_titanic.filter(lambda x: int(x["class"]) == 3)
first_class_list = [float(i["fare"]) for i in my_titanic_filtered_first_class.table]
third_class_list = [float(i["fare"]) for i in my_titanic_filtered_third_class.table]
print(
    "The Average of Fare of first class", sum(first_class_list) / len(first_class_list)
)
print(
    "The Average of Fare of third class", sum(third_class_list) / len(third_class_list)
)

print("Filter Titanic Task 1.5")
my_titanic_filtered_m_survived = my_titanic.filter(
    lambda x: x["gender"] == "M" and x["survived"] == "yes"
)
my_titanic_filtered_f_survived = my_titanic.filter(
    lambda x: x["gender"] == "F" and x["survived"] == "yes"
)
print(
    f"The survived rate male {(len(my_titanic_filtered_m_survived.table) * 100) / len(my_titanic.table)} %"
)
print(
    f"The survived rate female {(len(my_titanic_filtered_f_survived.table) * 100) / len(my_titanic.table)} %"
)
