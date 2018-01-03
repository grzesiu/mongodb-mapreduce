from bson.code import Code
from pymongo import MongoClient


def create(db, filename):
    with open(filename) as f:
        for line in f:
            db.groceries.insert({'items': line.strip().split(',')})


def get_counts(db):
    mapper = Code("""
    function() {
        for(var i = 0; i < this.items.length; i++) { 
            emit(this.items[i], 1);
        }
    }
    """)

    reducer = Code("""
    function(key, values) {
        var total = 0;
        for(var i = 0; i < values.length; i++) {
            total += values[i];
        }
        return total;
    }
    """)

    db.groceries.map_reduce(mapper, reducer, 'counts')


def main():
    client = MongoClient()
    db = client.shopping
    # create(db, 'groceries.csv')
    get_counts(db)


if __name__ == '__main__':
    main()
