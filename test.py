import names
import database

def main():
    db = database.DB("test")

    user_columns = [
        database.Column(name="id", type="integer", primary=True),
        database.Column(name="username", type="string", size=20),
        database.Column(name="password", type="string", size=20)
    ]

    user_rows = [
        database.Row([1, "giuseppe", "password1"]),
        database.Row([2, "benedetta", "password2"]),
    ]

    for i in range(3, 100000):
        user_rows.append(database.Row([i, names.get_first_name(), f"password{i}"]))

    db.create_table("users", user_columns)
    db.insert_records("users", user_rows)
    db.save()

if __name__ == "__main__":
    main()