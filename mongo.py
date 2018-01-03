from pymongo import MongoClient


def create(db, filename):
    with open(filename) as f:
        for line in f:
            db.groceries.insert({'items': line.strip().split(',')})


def main():
    client = MongoClient()
    db = client.shopping
    create(db, 'groceries.csv')


if __name__ == '__main__':
    main()
