import math
import numpy as np
import pandas as pd
import pymongo

# Mongo client
client = pymongo.MongoClient("mongodb://localhost:27017")

# creating a "Student" database 
db = client["student"]

# UCI: Student Performance Data Set
# source: https://archive.ics.uci.edu/ml/datasets/Student+Performance 
# importing the Student dataset
df1 = pd.read_csv('./student/student-mat.csv', sep=';')

# first collection: with student-mat dataset
collection1 = db["student_mat"]

# coverting the df into the format insert_many takes
# this makes the list of dictionary for each entry 
all_records1 = df1.to_dict(orient='records')

# second collection: with student-por dataset
collection2 = db["student_por"]

# importing the Student dataset
df2 = pd.read_csv('./student/student-por.csv', sep=';')

# coverting the df into the format insert_many takes
# this makes the list of dictionary for each entry 
all_records2 = df2.to_dict(orient='records')


def insert_to_mongodb():
    # inserting the student performance record into the collection
    collection1.insert_many(all_records1)
    # inserting the student performance record into the collection
    collection2.insert_many(all_records2)
    print('Records inserted into the student databse -> collections')


if __name__ == '__main__':
    insert_to_mongodb()