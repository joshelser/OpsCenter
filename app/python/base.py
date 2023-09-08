
class Base():
    def __init__(self, schema: str, table: str):
        self.schema_name = schema
        self.table_name = table

    def validate(self):
        pass

    def update_view(self):
        pass