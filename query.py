class Query:
    def __init__(self, driver, source):
        self.driver = driver
        self.selected_fields = None
        self.condition = {'field': '', 'operator': '', 'value': ''}
        self.conditions = []
        self.source = source

    def select(self, list_of_fields=None):
        self.selected_fields = list_of_fields
        return self

    def where(self, field):
        self.condition['field'] = field
        return self

    def operation(self, operator, value):
        print("and the answer is 42")
        self.condition['operation'] = operator
        self.condition['value'] = value
        self.conditions.append(self.condition.copy())
        return self

    def lt(self, value):
        return self.operation(' < ', value)

    def gt(self, value):
        return self.operation(' > ', value)

    def eq(self, value):
        return self.operation(' = ', value)

    def generate_query(self):
        query = ''
        if self.driver == 'influx':
            # select part
            query = 'select '
            if not self.selected_fields:
                query += ' * '
            else:
                for field in self.selected_fields:
                    query += field + ','
                query = query[:-1]
            # from part
            query += 'from ' + self.source
            # where part
            if self.conditions:
                query += ' WHERE '
                for condition in self.conditions:
                    query += condition['field']
                    query += condition['operation']
                    query += str(condition['value'])
                    query += ' AND '
                query = query[:-4]

        return query


q = Query('influx', 'lexx')
q.select().where('test').gt(10)
q.where('me').eq(5)
print (q.generate_query())
