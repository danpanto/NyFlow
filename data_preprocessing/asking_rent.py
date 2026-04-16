from Crypto.SelfTest.Hash.test_SHAKE import data
import requests
import numpy as np
import pandas as pd
from pathlib import Path
import zipfile
import io

data_path = Path.cwd() / "data"
taxi_zones_path = data_path / "taxi_zone_lookup.csv"
if not taxi_zones_path.exists():
    exit(1)

raw_rent_path = data_path / "medianAskingRent_OneBd.csv"
output = data_path / "asking_rent_data.parquet"

if not raw_rent_path.exists():
    print("Descargando datos crudos")
    url = "https://cdn-charts.streeteasy.com/rentals/OneBd/medianAskingRent_OneBd.zip"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        zip_content = io.BytesIO(r.content)

        with zipfile.ZipFile(zip_content) as zf:
            zf.extractall(data_path)


df_rentas = pd.read_csv(raw_rent_path)
df_distritos = pd.read_csv(taxi_zones_path)

print("Iniciando imputación")
# Imputamos los datos
# 1. Definimos la jerarquía (Mapeo de barrios específicos a sus filas generales)
# Este diccionario vincula el 'areaName' específico con el 'areaName' general en tu df
jerarquia = {
    # Manhattan - Downtown
    "Chinatown": "All Downtown",
    "East Village": "All Downtown",
    "Financial District": "All Downtown",
    "Greenwich Village": "All Downtown",
    "Little Italy": "All Downtown",
    "Soho": "All Downtown",
    "Tribeca": "All Downtown",
    "West Village": "All Downtown",
    "Nolita": "All Downtown",
    # Manhattan - Midtown
    "Chelsea": "All Midtown",
    "Gramercy Park": "All Midtown",
    "Midtown East": "All Midtown",
    "Midtown West": "All Midtown",
    "Flatiron": "All Midtown",
    "Stuyvesant Town/PCV": "All Midtown",
    # Manhattan - Upper East Side
    "Upper East Side": "All Upper East Side",
    "Roosevelt Island": "All Upper East Side",
    # Manhattan - Upper West Side
    "Upper West Side": "All Upper West Side",
    # Manhattan - Upper Manhattan
    "Central Harlem": "All Upper Manhattan",
    "East Harlem": "All Upper Manhattan",
    "Hamilton Heights": "All Upper Manhattan",
    "Inwood": "All Upper Manhattan",
    "Morningside Heights": "All Upper Manhattan",
    "Washington Heights": "All Upper Manhattan",
    "West Harlem": "All Upper Manhattan",
}

# 2. Rellenar automáticamente el resto de distritos usando el Borough
# Si el barrio es de Brooklyn y no está en el dict, su padre será 'Brooklyn'
boroughs = ["Brooklyn", "Bronx", "Queens", "Staten Island"]


def obtener_padre(row):
    if row["areaName"] in jerarquia:
        return jerarquia[row["areaName"]]
    if row["Borough"] in boroughs:
        return row["Borough"]
    return "NYC"  # Fallback final


df_rentas["padre_imputacion"] = df_rentas.apply(obtener_padre, axis=1)

# 3. Proceso de Imputación Fila por Fila
cols_temporales = [c for c in df_rentas.columns if "-" in c]  # Columnas tipo '2010-01'

# Creamos un dataframe de referencia rápida indexado por el nombre del área
df_referencia = df_rentas.set_index("areaName")


def imputar_con_padre(row):
    padre_name = row["padre_imputacion"]

    # Si la fila no tiene NaNs, no hacemos nada
    if not row[cols_temporales].isna().any():
        return row

    # Intentamos obtener los datos del padre
    try:
        datos_padre = df_referencia.loc[padre_name, cols_temporales]
    except KeyError:
        return row  # Si el padre no existe en el df, saltar

    # Rellenamos los NaN del hijo con los valores del padre en esa columna
    for col in cols_temporales:
        if pd.isna(row[col]):
            row[col] = datos_padre[col]

    return row


# 4. Aplicamos la imputación
df_rentas = df_rentas.apply(imputar_con_padre, axis=1)

# 5. Limpieza final
# Si después de esto quedan NaNs (porque el padre también tenía NaNs),
# usamos una interpolación lineal para terminar de limpiar los bordes.
df_rentas[cols_temporales] = (
    df_rentas[cols_temporales]
    .interpolate(method="linear", axis=1)
    .bfill(axis=1)
    .ffill(axis=1)
)

print("Imputación jerárquica completada.")


print(
    "Mapeando zonas de taxi_zone_lookup.csv a las de los datos y mergeando para tener el id de zona con cada fila de los datos"
)
taxi_zone_mapping = {
    "Newark Airport": None,
    "Jamaica Bay": "Queens",
    "Allerton/Pelham Gardens": "Pelham Gardens",
    "Alphabet City": "East Village",
    "Arden Heights": "Staten Island",
    "Arrochar/Fort Wadsworth": "Staten Island",
    "Astoria": "Astoria",
    "Astoria Park": "Astoria",
    "Auburndale": "Auburndale",
    "Baisley Park": "South Jamaica",
    "Bath Beach": "Bath Beach",
    "Battery Park": "Manhattan",
    "Battery Park City": "Battery Park City",
    "Bay Ridge": "Bay Ridge",
    "Bay Terrace/Fort Totten": "Queens",
    "Bayside": "Bayside",
    "Bedford": "Bedford-Stuyvesant",
    "Bedford Park": "Bedford Park",
    "Bellerose": "Bellerose",
    "Belmont": "Belmont",
    "Bensonhurst East": "Bensonhurst",
    "Bensonhurst West": "Bensonhurst",
    "Bloomfield/Emerson Hill": "Staten Island",
    "Bloomingdale": "Manhattan",
    "Boerum Hill": "Boerum Hill",
    "Borough Park": "Borough Park",
    "Breezy Point/Fort Tilden/Riis Beach": "The Rockaways",
    "Briarwood/Jamaica Hills": "Briarwood",
    "Brighton Beach": "Brighton Beach",
    "Broad Channel": "Queens",
    "Bronx Park": "Bronx",
    "Bronxdale": "Bronx",
    "Brooklyn Heights": "Brooklyn Heights",
    "Brooklyn Navy Yard": "Brooklyn",
    "Brownsville": "Brownsville",
    "Bushwick North": "Bushwick",
    "Bushwick South": "Bushwick",
    "Cambria Heights": "Cambria Heights",
    "Canarsie": "Canarsie",
    "Carroll Gardens": "Carroll Gardens",
    "Central Harlem": "Central Harlem",
    "Central Harlem North": "Central Harlem",
    "Central Park": "Manhattan",
    "Charleston/Tottenville": "Staten Island",
    "Chinatown": "Chinatown",
    "City Island": "City Island",
    "Claremont/Bathgate": "Bronx",
    "Clinton East": "Chelsea",
    "Clinton Hill": "Clinton Hill",
    "Clinton West": "Midtown West",
    "Co-Op City": "Co-op City",
    "Cobble Hill": "Cobble Hill",
    "College Point": "College Point",
    "Columbia Street": "Columbia St Waterfront District",
    "Coney Island": "Coney Island",
    "Corona": "Corona",
    "Country Club": "Country Club",
    "Crotona Park": "Bronx",
    "Crotona Park East": "Crotona Park East",
    "Crown Heights North": "Crown Heights",
    "Crown Heights South": "Crown Heights",
    "Cypress Hills": "East New York",
    "DUMBO/Vinegar Hill": "DUMBO",
    "Douglaston": "Douglaston",
    "Downtown Brooklyn/MetroTech": "Downtown Brooklyn",
    "Dyker Heights": "Dyker Heights",
    "East Chelsea": "Chelsea",
    "East Concourse/Concourse Village": "Concourse",
    "East Elmhurst": "East Elmhurst",
    "East Flatbush/Farragut": "East Flatbush",
    "East Flatbush/Remsen Village": "East Flatbush",
    "East Flushing": "Flushing",
    "East Harlem North": "East Harlem",
    "East Harlem South": "East Harlem",
    "East New York": "East New York",
    "East New York/Pennsylvania Avenue": "East New York",
    "East Tremont": "East Tremont",
    "East Village": "East Village",
    "East Williamsburg": "Williamsburg",
    "Eastchester": "Eastchester",
    "Elmhurst": "Elmhurst",
    "Elmhurst/Maspeth": "Elmhurst",
    "Eltingville/Annadale/Prince's Bay": "Staten Island",
    "Erasmus": "Flatbush",
    "Far Rockaway": "The Rockaways",
    "Financial District North": "Financial District",
    "Financial District South": "Financial District",
    "Flatbush/Ditmas Park": "Flatbush",
    "Flatiron": "Flatiron",
    "Flatlands": "Flatlands",
    "Flushing": "Flushing",
    "Flushing Meadows-Corona Park": "Flushing",
    "Fordham South": "Fordham",
    "Forest Hills": "Forest Hills",
    "Forest Park/Highland Park": "Queens",
    "Fort Greene": "Fort Greene",
    "Fresh Meadows": "Fresh Meadows",
    "Freshkills Park": "Staten Island",
    "Garment District": "Midtown",
    "Glen Oaks": "Glen Oaks",
    "Glendale": "Glendale",
    "Governor's Island/Ellis Island/Liberty Island": "Manhattan",
    "Gowanus": "Gowanus",
    "Gramercy": "Gramercy Park",
    "Gravesend": "Gravesend",
    "Great Kills": "Staten Island",
    "Great Kills Park": "Staten Island",
    "Green-Wood Cemetery": "Greenwood",
    "Greenpoint": "Greenpoint",
    "Greenwich Village North": "Greenwich Village",
    "Greenwich Village South": "Greenwich Village",
    "Grymes Hill/Clifton": "Staten Island",
    "Hamilton Heights": "Hamilton Heights",
    "Hammels/Arverne": "The Rockaways",
    "Heartland Village/Todt Hill": "Staten Island",
    "Highbridge": "Highbridge",
    "Highbridge Park": "Manhattan",
    "Hillcrest/Pomonok": "Hillcrest",
    "Hollis": "Hollis",
    "Homecrest": "Brooklyn",
    "Howard Beach": "Howard Beach",
    "Hudson Sq": "Greenwich Village",
    "Hunts Point": "Hunts Point",
    "Inwood": "Inwood",
    "Inwood Hill Park": "Inwood",
    "JFK Airport": "Queens",
    "Jackson Heights": "Jackson Heights",
    "Jamaica": "Jamaica",
    "Jamaica Bay": "Queens",
    "Jamaica Estates": "Jamaica Estates",
    "Kensington": "Kensington",
    "Kew Gardens": "Kew Gardens",
    "Kew Gardens Hills": "Kew Gardens Hills",
    "Kingsbridge Heights": "Kingsbridge",
    "Kips Bay": "Gramercy Park",
    "LaGuardia Airport": "Queens",
    "Laurelton": "Laurelton",
    "Lenox Hill East": "Upper East Side",
    "Lenox Hill West": "Upper East Side",
    "Lincoln Square East": "Upper West Side",
    "Lincoln Square West": "Upper West Side",
    "Little Italy/NoLiTa": "Little Italy",
    "Long Island City/Hunters Point": "Long Island City",
    "Long Island City/Queens Plaza": "Long Island City",
    "Longwood": "Longwood",
    "Lower East Side": "Lower East Side",
    "Madison": "Brooklyn",
    "Manhattan Beach": "Manhattan Beach",
    "Manhattan Valley": "Manhattan",
    "Manhattanville": "West Harlem",
    "Marble Hill": "Marble Hill",
    "Marine Park/Floyd Bennett Field": "Marine Park",
    "Marine Park/Mill Basin": "Marine Park",
    "Mariners Harbor": "Staten Island",
    "Maspeth": "Maspeth",
    "Meatpacking/West Village West": "West Village",
    "Melrose South": "Melrose",
    "Middle Village": "Middle Village",
    "Midtown Center": "Midtown",
    "Midtown East": "Midtown East",
    "Midtown North": "Midtown",
    "Midtown South": "Midtown South",
    "Midwood": "Midwood",
    "Morningside Heights": "Morningside Heights",
    "Morrisania/Melrose": "Morrisania",
    "Mott Haven/Port Morris": "Mott Haven",
    "Mount Hope": "Bronx",
    "Murray Hill": "Midtown East",
    "Murray Hill-Queens": "Queens",
    "New Dorp/Midland Beach": "Staten Island",
    "Newark Airport": None,
    "North Corona": "North Corona",
    "Norwood": "Norwood",
    "Oakland Gardens": "Oakland Gardens",
    "Oakwood": "Staten Island",
    "Ocean Hill": "Brooklyn",
    "Ocean Parkway South": "Ocean Parkway",
    "Old Astoria": "Astoria",
    "Outside of NYC": None,
    "Ozone Park": "Ozone Park",
    "Park Slope": "Park Slope",
    "Parkchester": "Parkchester",
    "Pelham Bay": "Pelham Bay",
    "Pelham Bay Park": "Pelham Bay",
    "Pelham Parkway": "Pelham Parkway",
    "Penn Station/Madison Sq West": "Midtown",
    "Port Richmond": "Staten Island",
    "Prospect Heights": "Prospect Heights",
    "Prospect Park": "Prospect Park",
    "Prospect-Lefferts Gardens": "Prospect Lefferts Gardens",
    "Queens Village": "Queens Village",
    "Queensboro Hill": "Queens",
    "Queensbridge/Ravenswood": "Queens",
    "Randalls Island": "Manhattan",
    "Red Hook": "Red Hook",
    "Rego Park": "Rego Park",
    "Richmond Hill": "Richmond Hill",
    "Ridgewood": "Ridgewood",
    "Rikers Island": "Bronx",
    "Riverdale/North Riverdale/Fieldston": "Riverdale",
    "Rockaway Park": "The Rockaways",
    "Roosevelt Island": "Roosevelt Island",
    "Rosedale": "Rosedale",
    "Rossville/Woodrow": "Staten Island",
    "Saint Albans": "St. Albans",
    "Saint George/New Brighton": "Staten Island",
    "Saint Michaels Cemetery/Woodside": "Woodside",
    "Schuylerville/Edgewater Park": "Schuylerville",
    "Seaport": "Financial District",
    "Sheepshead Bay": "Sheepshead Bay",
    "SoHo": "Soho",
    "Soundview/Bruckner": "Soundview",
    "Soundview/Castle Hill": "Castle Hill",
    "South Beach/Dongan Hills": "Staten Island",
    "South Jamaica": "South Jamaica",
    "South Ozone Park": "South Ozone Park",
    "South Williamsburg": "Williamsburg",
    "Springfield Gardens North": "Springfield Gardens",
    "Springfield Gardens South": "Springfield Gardens",
    "Spuyten Duyvil/Kingsbridge": "Kingsbridge",
    "Stapleton": "Staten Island",
    "Starrett City": "Brooklyn",
    "Steinway": "Astoria",
    "Stuy Town/Peter Cooper Village": "Stuyvesant Town/PCV",
    "Stuyvesant Heights": "Bedford-Stuyvesant",
    "Sunnyside": "Sunnyside",
    "Sunset Park East": "Sunset Park",
    "Sunset Park West": "Sunset Park",
    "Sutton Place/Turtle Bay North": "Midtown East",
    "Times Sq/Theatre District": "Midtown",
    "TriBeCa/Civic Center": "Tribeca",
    "Two Bridges/Seward Park": "Manhattan",
    "UN/Turtle Bay South": "Midtown East",
    "Union Sq": "Manhattan",
    "University Heights/Morris Heights": "University Heights",
    "Upper East Side North": "Upper East Side",
    "Upper East Side South": "Upper East Side",
    "Upper West Side North": "Upper West Side",
    "Upper West Side South": "Upper West Side",
    "Van Cortlandt Park": "Bronx",
    "Van Cortlandt Village": "Bronx",
    "Van Nest/Morris Park": "Van Nest",
    "Washington Heights North": "Washington Heights",
    "Washington Heights South": "Washington Heights",
    "West Brighton": "Staten Island",
    "West Chelsea/Hudson Yards": "Chelsea",
    "West Concourse": "Concourse",
    "West Farms/Bronx River": "Bronx",
    "West Village": "West Village",
    "Westchester Village/Unionport": "Westchester Village",
    "Westerleigh": "Staten Island",
    "Whitestone": "Whitestone",
    "Willets Point": "Queens",
    "Williamsbridge/Olinville": "Williamsbridge",
    "Williamsburg (North Side)": "Williamsburg",
    "Williamsburg (South Side)": "Williamsburg",
    "Windsor Terrace": "Windsor Terrace",
    "Woodhaven": "Woodhaven",
    "Woodlawn/Wakefield": "Woodlawn",
    "Woodside": "Woodside",
    "World Trade Center": "Financial District",
    "Yorkville East": "Upper East Side",
    "Yorkville West": "Upper East Side",
}

df_distritos["areaName"] = df_distritos["Zone"].map(taxi_zone_mapping)

df_merged = df_rentas.merge(df_distritos, on="areaName", how="inner")
df_merged = df_merged[cols_temporales + ["LocationID"]]

print("Transformando los datos a formato largo con fechas")
df_final = pd.melt(
    df_merged, id_vars=["LocationID"], var_name="Date", value_name="AskingRent"
)
df_final["Date"] = pd.to_datetime(df_final["Date"], format="%Y-%m")

print(f"Guardando en {output}")
df_final.to_parquet(output, index=False)
print("Hecho")
