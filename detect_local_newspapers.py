import pandas as pd

# The next file contains the locations of all edeka stores within Germany
# obtained from mein-prospekt.de
edeka_locations = pd.read_csv("Edeka_Locations.csv")
# This files contains all german cities with their respective zip-code
german_cities = pd.read_csv("German_cities.csv")
# These are all local german newspapers
local_german_newspaper = pd.read_csv("local_newspaper.csv")

# Obtain all zip codes of cities where Kohler has a store
kohler_stores = [edeka_locations["zip-code"].iloc[i]
                 for i in range(len(edeka_locations)) if "Kohler" in edeka_locations["name"][i]]

# Get all german zip codes starting with a 77, since 17 stores of Kohler are in that area
sevenseven_area_code = german_cities["PLZ"][german_cities["PLZ"].str.startswith(
    "77")]
# Get all cities names using the sevenseven_area_code
sevenseven_cities = german_cities["Stadt"][german_cities["PLZ"].isin(
    sevenseven_area_code)]
# Get all regional newspapers within those cities
sevenseven_newspapers = local_german_newspaper["Name"][local_german_newspaper["Redaktionssitz"].isin(
    sevenseven_cities)]
sevenseven_newspapers

# Get all german zip codes with a 79, since 3 stores of Kohler are in that area
sevennine_area_code = german_cities["PLZ"][german_cities["PLZ"].str.startswith(
    "79")]
# Get all cities names using the sevenseven_area_code
sevennine_cities = german_cities["Stadt"][german_cities["PLZ"].isin(
    sevennine_area_code)]
# Get all regional newspapers within those cities
sevennine_newspapers = local_german_newspaper["Name"][local_german_newspaper["Redaktionssitz"].isin(
    sevennine_cities)]
sevennine_newspapers
