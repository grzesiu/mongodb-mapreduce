class Rule:
    def __init__(self, i1, i2, conf, sup):
        self.i1 = i1
        self.i2 = i2
        self.conf = conf
        self.sup = sup

    def __str__(self):
        return '{} - > {}: {}, {}'.format(self.i1, self.i2, self.conf, self.sup)

    def __repr__(self):
        return self.__str__()
