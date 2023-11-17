import os
import struct
import pickle
import bintrees

class Column:
    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.type = kwargs["type"]
        self.primary = False

        if "primary" in kwargs and type(kwargs["primary"]) == bool:
            self.primary = kwargs["primary"]

        match self.type:
            case "string":
                if kwargs["size"] and type(kwargs["size"]) == int:
                    self.size = kwargs["size"]
                else:
                    assert False, "size must be specified for a string and must be an integer"
            case "integer" | "unsigned":
                self.size = 8
            case _:
                assert False, "unrecognized type"
            
    def __repr__(self):
        return f"Column(name={self.name}, type={self.type}, size={self.size})"
    
class Row:
    def __init__(self, columns):
        self.columns = columns

    def __getitem__(self, index):
        return self.columns[index]

    def __setitem__(self, index, value):
        self.columns[index] = value

    def __repr__(self):
        return repr(self.columns)

class Index:
    def __init__(self):
        self.index = bintrees.FastAVLTree()

    def insert(self, key, value):
        self.index.insert(key, value)
    
    def search(self, key):
        return self.index.get(key)
    
    def delete(self, key):
        self.index.remove(key)

    def keys(self):
        return self.index.keys()
    
    def print(self):
        for key in self.index.keys():
            print(key, self.index.get(key))

class Table:
    def __init__(self, name):
        self.name = name

    def create(self, columns):
        self.columns = columns
        self.row_size = 0
        self.num_records = 0

        with open(f"{self.name}.tbl", "wb") as f:
            f.write(struct.pack("20s", "MyDBv0.1".encode("utf-8")))
            f.write(struct.pack("B", len(columns)))

            for column in columns:
                f.write(struct.pack("32s", column.name.encode("utf-8")))
                f.write(struct.pack("16s", column.type.encode("utf-8")))
                f.write(struct.pack("B", column.size))
                self.row_size += column.size

            self.start = f.tell()
            f.write(struct.pack("Q", 0))

    def load(self):
        self.row_size = 0
        with open(f"{self.name}.tbl", "rb") as f:
            magic = struct.unpack("20s", f.read(20))[0].decode("utf-8").rstrip("\x00")

            if magic != "MyDBv0.1":
                assert False, "invalid database file"

            self.columns = []
            num_columns = struct.unpack("B", f.read(1))[0]

            for _ in range(num_columns):
                name = struct.unpack("32s", f.read(32))[0].decode("utf-8").rstrip("\x00")
                type = struct.unpack("16s", f.read(16))[0].decode("utf-8").rstrip("\x00")
                size = struct.unpack("B", f.read(1))[0]
                self.columns.append(Column(name=name, type=type, size=size))
                self.row_size += size

            self.start = f.tell()
            self.num_records = struct.unpack("Q", f.read(8))[0]

    def write_record(self, f, record):
        for i, column in enumerate(self.columns):
            if column.type == "string":
                f.write(struct.pack(f"{column.size}s", record[i].encode("utf-8")))
            elif column.type == "integer":
                f.write(struct.pack("q", record[i]))
            elif column.type == "unsigned":
                f.write(struct.pack("Q", record[i]))
            else:
                assert False, "unrecognized type"

    def insert_records(self, records):
        with open(f"{self.name}.tbl", "r+b") as f:
            f.seek(self.start)
            f.write(struct.pack("Q", self.num_records + len(records)))

            for record in records:
                f.seek(self.start + 8 + self.num_records * self.row_size)

                self.write_record(f, record)
                self.num_records += 1

    def insert_record(self, record):
        self.insert_records([record])

    def print_records(self):
        with open(f"{self.name}.tbl", "rb") as f:
            f.seek(self.start + 8)
            for i in range(self.num_records):
                for column in self.columns:
                    if column.type == "string":
                        print(struct.unpack(f"{column.size}s", f.read(column.size))[0].decode("utf-8").rstrip("\x00"), end=" ")
                    elif column.type == "integer":
                        print(struct.unpack("q", f.read(8))[0], end=" ")
                    elif column.type == "unsigned":
                        print(struct.unpack("Q", f.read(8))[0], end=" ")
                    else:
                        assert False, "unrecognized type"
                print()
    
    def read_record(self, position):
        with open(f"{self.name}.tbl", "rb") as f:
            f.seek(self.start + 8 + position * self.row_size)
            values = []
            for column in self.columns:
                if column.type == "string":
                    values.append(struct.unpack(f"{column.size}s", f.read(column.size))[0].decode("utf-8").rstrip("\x00"))
                elif column.type == "integer":
                    values.append(struct.unpack("q", f.read(8))[0])
                elif column.type == "unsigned":
                    values.append(struct.unpack("Q", f.read(8))[0])
            return tuple(values)

class DB:
    def __init__(self, name):
        self.name = name
        self.db_file = f"{self.name}.db"
        self.tables = {}
        self.database = {
            "tables_names": [],
            "indexes": {}
        }

        if os.path.exists(self.db_file):
            with open(self.db_file, "rb") as f:
                self.database = pickle.load(f)

        for table_name in self.database["tables_names"]:
            table = Table(table_name)
            table.load()
            self.tables[table_name] = table
            print("Loaded table", table_name)

    def create_table(self, name, columns):
        table = Table(name)
        table.create(columns)

        print(columns)

        for column in columns:
            if column.primary:
                if name not in self.database["indexes"]:
                    self.database["indexes"][name] = {}
                    print("Creo lista indici per tabella", name)
                self.database["indexes"][name][column.name] = Index()
                print("Creo indice per tabella", name, "e colonna", column.name)

        self.database["tables_names"].append(name)
        self.tables[name] = table

    def insert_record(self, table_name, record):
        table = self.tables[table_name]
        table.insert_record(record)

        print("Inserito record in tabella", table_name, "con valori", record)

        for column in self.database["indexes"][table_name].keys():
            for i, column_name in enumerate(table.columns):
                print(column)
                if column_name.name == column:
                    print("yo")
                    self.database["indexes"][table_name][column].insert(record[i], table.num_records - 1)
                    break

    def insert_records(self, table_name, records):
        for record in records:
            self.insert_record(table_name, record)

    def read_records(self, table_name):
        table = self.tables[table_name]
        table.print_records()

    def search(self, table_name, column, key):
        table = self.tables[table_name]
        position = self.database["indexes"][table_name][column].search(key)
        if position is None:
            return None
        print(table.read_record(position))

    def save(self):
        with open(self.db_file, "wb") as f:
            pickle.dump(self.database, f)
