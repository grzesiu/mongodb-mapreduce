from functools import partial

from bson.code import Code
from pymongo import MongoClient

from rule import Rule


def create(db, filename):
    with open(filename) as f:
        for line in f:
            db.groceries.insert({'items': sorted(line.strip().split(','))})


def count_items(db):
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

    db.groceries.map_reduce(mapper, reducer, 'item_counts')


def count_pairs(db):
    mapper = Code("""
        function() {
            if(this.items.length > 1) {
                for(var i = 0; i < this.items.length; i++) {
                    for(var j = 0; j < this.items.length; j++) {
                        if(i != j) {
                            emit({1: this.items[i], 2: this.items[j]}, 1);
                        }
                    }
                }
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

    db.groceries.map_reduce(mapper, reducer, 'pair_counts')


def get_item_counts(db):
    item_counts = {}
    for i in db.item_counts.find():
        item_counts[i['_id']] = i['value']
    return item_counts


def get_rules(db):
    rules = []
    item_counts = get_item_counts(db)

    for i in db.pair_counts.find():
        conf = i['value'] / item_counts[i['_id']['1']]
        sup = i['value'] / db.groceries.count()
        rules.append(Rule(i['_id']['1'], i['_id']['2'], conf, sup))

    return rules


def get_passes(rules, conf, sup):
    return sum(rule.conf > conf and rule.sup > sup for rule in rules)


def main():
    client = MongoClient()
    db = client.shopping
    # create(db, 'groceries.csv')
    # count_items(db)
    # count_pairs(db)
    confs = [0.01, 0.25, 0.50, 0.75, 0.25, 0.25, 0.25, 0.25]
    sups = [0.01, 0.01, 0.01, 0.01, 0.05, 0.07, 0.20, 0.50]

    rules = get_rules(db)
    passes = list(map(partial(get_passes, rules), confs, sups))
    print(passes)


if __name__ == '__main__':
    main()
