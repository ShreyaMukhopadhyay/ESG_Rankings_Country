import numpy as np
import pandas as pd

# sharepoint_path = r"/Users/wrngnfreeman/Library/CloudStorage/OneDrive-Personal/Women in AI/ESG_CSV"
sharepoint_path = r"/Users/shreya/Library/CloudStorage/OneDrive-Personal/Work/Women in AI/ESG_CSV"
# sharepoint_path = r"C:/Users/shrey/OneDrive/Work/Women in AI/ESG_CSV"


# Importing datasets
world_bank_data = pd.read_excel(
    io=sharepoint_path + r"/raw_data/ESGEXCEL.xlsx",
    sheet_name=r"Data",
    engine = "openpyxl"
)
metrics_data = pd.read_excel(
    io=sharepoint_path + r"/raw_data/ESGEXCEL.xlsx",
    sheet_name=r"Series",
    engine="openpyxl"
)
region_data = pd.read_excel(
    io=sharepoint_path + r"/raw_data/ESGEXCEL.xlsx",
    usecols=["Table Name", "Region", "Income Group", "Lending category"],
    sheet_name=r"Country",
    engine="openpyxl"
)
continents_data = pd.read_excel(
    io=sharepoint_path + r"/intermediate_files/regions.xlsx",
    sheet_name=r"Sheet1",
    engine = "openpyxl"
)

# Data Preparation
## transforming data into long format
long_data = pd.melt(
    frame=world_bank_data,
    id_vars=["Country Name", "Country Code", "Indicator Name", "Indicator Code"]
    # value_vars=np.arange(start=2000, stop=2021, step=1).tolist()
).rename(
    columns={
        "variable": "Year"
    }
)
## keeping only countries and removing group of countries (or regions like "Arab World")
regions = [
    "Arab World",
    "Caribbean small states",
    "Central Europe and the Baltics",
    "Early-demographic dividend",
    "East Asia & Pacific",
    "East Asia & Pacific (IDA & IBRD)",
    "East Asia & Pacific (excluding high income)",
    "Euro area",
    "Europe & Central Asia",
    "Europe & Central Asia (IDA & IBRD)",
    "Europe & Central Asia (excluding high income)",
    "European Union",
    "Fragile and conflict affected situations",
    "Heavily indebted poor countries (HIPC)",
    "High income",
    "IBRD only",
    "IDA & IBRD total",
    "IDA blend",
    "IDA only",
    "IDA total",
    "Late-demographic dividend",
    "Latin America & Caribbean",
    "Latin America & Caribbean (IDA & IBRD)",
    "Latin America & Caribbean (excluding high income)",
    "Least developed countries: UN classification",
    "Low & middle income",
    "Low income",
    "Lower middle income",
    "Middle East & North Africa",
    "Middle East & North Africa (IDA & IBRD)",
    "Middle East & North Africa (excluding high income)",
    "Middle income",
    "North America",
    "OECD members",
    "Other small states",
    "Pacific island small states",
    "Post-demographic dividend",
    "Pre-demographic dividend",
    "Small states",
    "South Asia",
    "South Asia (IDA & IBRD)",
    "Sub-Saharan Africa",
    "Sub-Saharan Africa (IDA & IBRD)",
    "Sub-Saharan Africa (excluding high income)",
    "Upper middle income",
    "World"
]
long_data = long_data.loc[
    ~long_data["Country Name"].isin(regions),
    :
]
## renaming some countries
country_names ={
    "Bahamas, The": "The Bahamas",
    "Brunei Darussalam": "Brunei",
    "Cabo Verde": "Cape Verde",
    "Cote d'Ivoire": "Ivory Coast",
    "Côte d'Ivoire": "Ivory Coast",
    "Congo, Dem. Rep.": "Democratic Republic of the Congo",
    "Congo, Rep.": "Republic of the Congo",
    "Czechia": "Czech Republic",
    "Egypt, Arab Rep.": "Egypt",
    "Gambia, The": "The Gambia",
    "Iran, Islamic Rep.": "Iran",
    "Korea, Dem. People's Rep.": "North Korea",
    "Korea, Rep.": "South Korea",
    "Kyrgyz Republic": "Kyrgyzstan",
    "Lao PDR": "Laos",
    "Micronesia, Fed. Sts.": "Micronesia",
    "Russian Federation": "Russia",
    "Slovak Republic": "Slovakia",
    "St. Kitts and Nevis": "Saint Kitts and Nevis",
    "St. Lucia": "Saint Lucia",
    "St. Vincent and the Grenadines": "Saint Vincent and the Grenadines",
    "Syrian Arab Republic": "Syria",
    "São Tomé and Principe": "Sao Tome and Principe",
    "Timor-Leste": "East Timor",
    "Turkiye": "Turkey",
    "Türkiye": "Turkey",
    "United States": "United States of America",
    "Venezuela, RB": "Venezuela",
    "Yemen, Rep.": "Yemen"
}


## Preparing metric category and sub-categories
metrics_data[["Category", "Sub-Category"]] = metrics_data["Topic"].str.split(r": ", n=1, expand=True)
metrics_data["Indicator Name"] = [
    r"Tree Cover Loss (hectares)" if metrics_data.loc[i, "Indicator Name"] == r"Tree Cover Loss"\
    else r"Access to clean fuels and technologies for cooking (% of population)" if metrics_data.loc[i, "Indicator Name"] == r"Access to clean fuels and technologies for cooking  (% of population)"\
    else metrics_data.loc[i, "Indicator Name"]\
    for i in metrics_data.index
]
metrics_data = metrics_data[["Category", "Sub-Category", "Indicator Name"]].sort_values(by=["Category", "Sub-Category", "Indicator Name"])
metrics_data.dropna(how="any", inplace=True)

## Preparing a dataframe of regions that contains continents, DHS regions, etc.
region_data = region_data.loc[
    ~region_data["Table Name"].isin(regions),
    :
]
region_data["Table Name"] = [country_names.get(item, item) for item in region_data["Table Name"]]
region_data = pd.merge(
    left=continents_data,
    right=region_data.rename(columns={"Table Name": "Country"}),
    how="outer",
    left_on=["Country"],
    right_on=["Country"]
)



## Selecting 193 countries
long_data["Country Name"] = [country_names.get(item, item) for item in long_data["Country Name"]]
long_data["Year"] = long_data["Year"].apply(int)
long_data = long_data.sort_values(
    by=["Country Name", "Indicator Code", "Year"],
    ascending=[True, True, True]
).reset_index(drop=True)

# Exporting long format dataframe
with pd.ExcelWriter(
    path=sharepoint_path + r"/intermediate_files/ESG_data.xlsx",
    engine="openpyxl",
    mode="w",
    date_format='DD-MMM-YYYY',
    datetime_format='DD-MMM-YYYY'
) as writer:
    long_data[["Country Name", "Indicator Name", "Year", "value"]].to_excel(
        excel_writer=writer,
        index=False,
        sheet_name="ESG_data"
    )
    metrics_data.to_excel(
        excel_writer=writer,
        index=False,
        sheet_name="Metrics"
    )
    region_data.to_excel(
        excel_writer=writer,
        index=False,
        sheet_name="Regions"
    )

# Data preparation 2
## replacing 0's with NaNs
long_data = long_data[["Country Name", "Indicator Name", "Year", "value"]].replace(0, np.nan)

## Subsetting data for aggregation
long_data = long_data.loc[
    (long_data["Year"] >= 2015) & (long_data["Year"] <= 2020),
    :
]

## Listing features that are to bne ranked
positive_impacts = [
    r"Renewable electricity output (% of total electricity output)",
    r"Renewable energy consumption (% of total final energy consumption)",

    r"Proportion of bodies of water with good ambient water quality",

    r"Coastal protection",
    r"Terrestrial and marine protected areas (% of total territorial area)",

    r"GDP growth (annual %)",
    r"Individuals using the Internet (% of population)",

    r"Government Effectiveness: Estimate",
    r"Regulatory Quality: Estimate",

    r"Economic and Social Rights Performance Score",
    r"Strength of legal rights index (0=weak to 12=strong)",
    r"Voice and Accountability: Estimate",

    r"Patent applications, residents",
    r"Research and development expenditure (% of GDP)",
    r"Scientific and technical journal articles",

    r"Control of Corruption: Estimate",
    r"Political Stability and Absence of Violence/Terrorism: Estimate",
    r"Rule of Law: Estimate",

    r"Access to clean fuels and technologies for cooking (% of population)",
    r"Access to electricity (% of population)",
    r"People using safely managed drinking water services (% of population)",
    r"People using safely managed sanitation services (% of population)",

    r"Fertility rate, total (births per woman)",
    r"Life expectancy at birth, total (years)",

    r"Government expenditure on education, total (% of government expenditure)",
    r"School enrollment, primary (% gross)",

    r"Hospital beds (per 1,000 people)",

    r"Income share held by lowest 20%"
]
negative_impacts = [
    r"CO2 emissions (metric tons per capita)",
    r"Methane emissions (metric tons of CO2 equivalent per capita)",
    r"Nitrous oxide emissions (metric tons of CO2 equivalent per capita)",
    r"PM2.5 air pollution, mean annual exposure (micrograms per cubic meter)",

    r"Electricity production from coal sources (% of total)",
    r"Fossil fuel energy consumption (% of total)",

    r"Food production index (2014-2016 = 100)",

    r"Adjusted savings: natural resources depletion (% of GNI)",
    r"Adjusted savings: net forest depletion (% of GNI)",
    r"Annual freshwater withdrawals, total (% of internal resources)",
    r"Mammal species, threatened",

    r"Proportion of seats held by women in national parliaments (%)",
    r"Ratio of female to male labor force participation rate (%) (modeled ILO estimate)",
    r"School enrollment, primary and secondary (gross), gender parity index (GPI)",

    r"Unemployment, total (% of total labor force) (modeled ILO estimate)",

    r"Cause of death, by communicable diseases and maternal, prenatal and nutrition conditions (% of total)",
    r"Mortality rate, under-5 (per 1,000 live births)",
    r"Prevalence of overweight (% of adults)",
    r"Prevalence of undernourishment (% of population)",

    r"Gini index",
    r"Poverty headcount ratio at national poverty lines (% of population)"
]
long_data = long_data.loc[long_data["Indicator Name"].isin(positive_impacts+negative_impacts), :]


## Transforming features to represent equality better
long_data.loc[
    long_data["Indicator Name"] == r"Proportion of seats held by women in national parliaments (%)",
    "value"
] = abs(50 - long_data.loc[
    long_data["Indicator Name"] == r"Proportion of seats held by women in national parliaments (%)",
    "value"
])
long_data.loc[
    long_data["Indicator Name"] == r"Ratio of female to male labor force participation rate (%) (modeled ILO estimate)",
    "value"
] = abs(100 - long_data.loc[
    long_data["Indicator Name"] == r"Ratio of female to male labor force participation rate (%) (modeled ILO estimate)",
    "value"
])
long_data.loc[
    long_data["Indicator Name"] == r"School enrollment, primary and secondary (gross), gender parity index (GPI)",
    "value"
] = abs(1 - long_data.loc[
    long_data["Indicator Name"] == r"School enrollment, primary and secondary (gross), gender parity index (GPI)",
    "value"
])

## Aggregating features across years and at country level
long_data = long_data.groupby(
    by=[
        r"Country Name",
        r"Indicator Name"
    ]
).agg({"value": "median"}).reset_index(drop=False)

## Ranking features (lower the better)
long_data = long_data.sort_values(by=["Country Name", "Indicator Name"]).reset_index(drop=True)
for indicator in negative_impacts:
    long_data.loc[
        long_data["Indicator Name"] == indicator,
        "value"
    ] = long_data.loc[
        long_data["Indicator Name"] == indicator,
        "value"
    ].rank(
        method="first",
        ascending=True
    )
del indicator
for indicator in positive_impacts:
    long_data.loc[
        long_data["Indicator Name"] == indicator,
        "value"
    ] = long_data.loc[
        long_data["Indicator Name"] == indicator,
        "value"
    ].rank(
        method="first",
        ascending=False
    )
del indicator
long_data.fillna(value=193, inplace=True)

# Exporting datasets
with pd.ExcelWriter(
    path=sharepoint_path + r"/intermediate_files/ESG_data_ranked.xlsx",
    engine="openpyxl",
    mode="w",
    date_format='DD-MMM-YYYY',
    datetime_format='DD-MMM-YYYY'
) as writer:
    long_data.to_excel(
        excel_writer=writer,
        index=False,
        sheet_name="ESG_data - Ranked"
    )
    metrics_data.to_excel(
        excel_writer=writer,
        index=False,
        sheet_name="Metrics"
    )
    region_data.to_excel(
        excel_writer=writer,
        index=False,
        sheet_name="Regions"
    )
