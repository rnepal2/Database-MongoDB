import math
import pandas as pd
import pymongo


# Mongo client
client = pymongo.MongoClient("mongodb://localhost:27017")

# preparing dataset to insert into the mongo database
# Here we will use: Global Terrorism Database events from 2013-2017

# importing the gtd events in between 2013 and 2017
df = pd.read_excel('./terrorism/gtd_2014to2017.xlsx', sep=';')

# columns to keep in the mongo database
columns = ['iyear', 'imonth', 'iday', 'country_txt', 'region_txt', 'provstate', 'city', 'latitude', 
           'longitude', 'location', 'summary', 'crit1', 'crit2', 'crit3', 'alternative_txt', 'multiple',
           'success', 'suicide', 'attacktype1_txt', 'attacktype2_txt', 'attacktype3_txt', 'targtype1_txt', 
           'targsubtype1_txt', 'corp1', 'target1', 'natlty1_txt', 'targtype2_txt',  'targsubtype2_txt', 
           'corp2', 'target2', 'natlty2_txt', 'targtype3_txt', 'targsubtype3_txt', 'corp3', 'target3',
           'natlty3_txt', 'gname', 'gsubname', 'gname2', 'gsubname2', 'gname3', 'gsubname3', 'motive', 
           'guncertain1', 'guncertain2', 'guncertain3', 'individual', 'nperps', 'nperpcap', 'claimed',
           'claimmode', 'claimmode_txt', 'claim2', 'claimmode2', 'claimmode2_txt', 'claim3', 'claimmode3', 
           'claimmode3_txt', 'compclaim',  'weaptype1_txt', 'weapsubtype1_txt',  'weaptype2_txt', 
           'weapsubtype2_txt',  'weaptype3_txt',  'weapsubtype3_txt', 'weapdetail', 'nkill', 'nkillus', 
           'nkillter', 'nwound', 'nwoundus', 'nwoundte', 'property', 'propextent_txt', 'propvalue', 
           'propcomment', 'ishostkid', 'nhostkid', 'nhostkidus', 'nhours', 'ndays', 
           'ransom', 'ransomamt', 'ransomamtus', 'ransompaid', 'ransompaidus', 'hostkidoutcome_txt',
           'nreleased']

df = df[columns]

# introducing new column as a GeoObject: representing the incident location
# location object  →  GeoObject
'''
    loc: {
        coordinates: [longitude, latitude]
    }
'''
df['loc'] = df.apply(lambda row: {"type": "Point", "coordinate" : [row.longitude, row.latitude]}, 
                          axis=1)
# Now dropping the old latitude and longitude columns
df.drop(columns=['longitude', 'latitude'], inplace=True)

# removes the items from the dictionary (each record) where values are nan
def remove_nan_records(records):
    all_events = []
    for event in records:
        cleaned_event = {}
        for (key, value) in event.items():
            if key != 'loc' and str(value) != 'nan':
                cleaned_event[key] = value
            if key == 'loc':
                if str(value[0]) != 'nan' and str(value[1]) != 'nan':
                    cleaned_event[key] = value
        all_events.append(cleaned_event)
    return all_events


# if database already exists: drop it before creating it again
if 'terrorism' in client.list_database_names():
    client.drop_database('terrorism')

# inserts events into the mongo database
def insert_to_mongodb():
    # coverting the df into the format insert_many takes
    # this makes the list of dictionary for each entry 
    records = df.to_dict(orient='records')
    # all events to push to mongo database: terrorism-> incidents
    events_list = remove_nan_records(records=records)
    # creating a "terrorism" database 
    db = client["terrorism"]
    # creating collection: in terrorism database
    collection = db["incidents"]
    # inserting the student performance record into the collection
    collection.insert_many(events_list)
    print('Terrorism incidents inserted into terrorism database-> collection')


if __name__ == '__main__':
    insert_to_mongodb()
    
