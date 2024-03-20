import pandas as pd
from pymongo import MongoClient
from pymongo.collection import ReturnDocument

# Load the datasets
reductions_df = pd.read_csv('data/combined_reductions_report.csv')
emissions_df = pd.read_csv('data/combined_emission_report.csv')
assessment_df = pd.read_csv('data/2023_Cities_Climate_Risk_and_Vulnerability_Assessments_20240207.csv')

assessment_df.rename(columns={'Country/Area': 'Country'}, inplace=True)

# Merge datasets on Country and City, assuming consistent naming
merged_df = pd.merge(reductions_df, emissions_df,
                     on=['Country', 'City', 'C40', 'Reporting Year', 'City Location', 'Country Location'], how='outer')

full_merged_df = pd.merge(merged_df, assessment_df, on=['Country', 'City'], how='outer')
full_merged_df.dropna(inplace=True)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['environment_data']
city_reports_collection = db['city_reports']


# Function to create or update a document in MongoDB
def upsert_city_report(row):
    # Prepare the document structure
    document = {
        "$setOnInsert": {
            "country": row['Country'],
            "city": row['City'],
            "c40": row['C40'],
            "location": {
                "city": row['City Location_x'],
                "country": row['Country Location']
            }
        },
        "$addToSet": {
            "reports": {
                "reportingYear": row['Reporting Year'],
                "baselineYear": row.get('Baseline year', None),
                "baselineEmissions": row.get('Baseline emissions', None),
                "percentageReductionTarget": row.get('Percentage reduction target', None),
                "targetDate": row.get('Target date', None),
                "totalEmissions": row.get('Total Emissions', None)
            },
            "climateRiskAssessments": {
                "yearOfPublication": row.get('Year of publication or approval', None),
                "factorsConsidered": row.get('Factors considered in assessment', None),
                "primaryAuthors": row.get('Primary author(s) of assessment', None),
                "adaptationGoals": row.get('Does the city have adaptation goal(s) and/or an adaptation plan?', None),
                "population": row.get('Population', None),
                "populationYear": row.get('Population Year', None),
                "lastUpdate": row.get('Last update', None)
            }
        }
    }

    # Upsert the document in the collection
    city_reports_collection.find_one_and_update(
        {"country": row['Country'], "city": row['City']},
        document,
        upsert=True,
        return_document=ReturnDocument.AFTER
    )


def do_upsert():
    # Iterate through the merged dataframe and upsert documents
    full_merged_df.apply(upsert_city_report, axis=1)
