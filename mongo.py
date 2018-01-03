from pymongo import MongoClient

client = MongoClient()
db = client.shopping


with open('groceries.csv') as f:
    for line in f:
        db.groceries.insert({'items': line.strip().split(',')})
