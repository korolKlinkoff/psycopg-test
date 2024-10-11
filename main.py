import psycopg2
import config


def detuple(item): return [i[0] for i in item]


def create_tables(cursor):
    cursor.execute("""
    create table if not exists Client (
    id serial primary key,
    name varchar(60),
    lastname varchar(60),
    email varchar(60) unique
    );
    """)
    cursor.execute("""
    create table if not exists PhoneNumbers (
    id serial primary key,
    number bigint unique,
    client_id integer references client(id)
    );
    """)


def add_client(cursor, name, lastname, email):
    cursor.execute("""
    insert into client (name, lastname, email)
    values (%s, %s, %s);
    """, (name, lastname, email))


def add_phone_number(cursor, phone_number, client_id):
    cursor.execute("""
insert into phonenumbers (number, client_id)
values (%s, %s);
    """, (phone_number, client_id))


def find_client_id(cursor, name=None, lastname=None, email=None, phone_number=None):
    # email is unique so this always return one result
    if email:
        cursor.execute("""
select id from client where email = %s;
    """, (email,))
        return detuple(cursor.fetchall())

    # phone number can't have more than one client id
    if phone_number:
        cursor.execute("""
select client_id from phonenumbers where number = %s;
        """, (phone_number,))
        return detuple(cursor.fetchall())

    # following returns all results
    results = []
    if name:
        cursor.execute("""
select id from client where name = %s;
    """, (name,))
        results = list(cursor.fetchall())
        if lastname:
            cursor.execute("""
            select id from client where lastname = %s;
            """, (lastname,))
            results = list(set(results).intersection(cursor.fetchall()))
    elif lastname:
        if lastname:
            cursor.execute("""
            select id from client where lastname = %s;
            """, (lastname,))
            results = list(cursor.fetchall())
    return detuple(results)


def update_client(cursor, client_id, name=None, lastname=None, email=None):
    if name:
        cursor.execute("""
        update client set name = %s where id = %s;
        """, (name, client_id))
    if lastname:
        cursor.execute("""
        update client set lastname = %s where id = %s;
        """, (name, client_id))
    if email:
        cursor.execute("""
        update client set email = %s where id = %s;
        """, (name, client_id))


def delete_phone(cursor, client_id=None, phone_number=None):
    if phone_number:
        cursor.execute("""
        delete from phonenumbers where number = %s;
        """, (phone_number,))
    if client_id:
        cursor.execute("""
        delete from phonenumbers where client_id = %s;
        """, (client_id,))


def delete_client(cursor, client_id):
    delete_phone(cursor, client_id=client_id)
    cursor.execute("""
    delete from client where id = %s;
    """, (client_id,))


if __name__ == "__main__":
    with psycopg2.connect(database=config.database, user=config.user, password=config.password) as conn:
        with conn.cursor() as cur:
            create_tables(cur)
            add_client(cur, "vasya", "pupkin", "vasya@email.com")
            add_client(cur, "vasya", "pupkin", "vasya2@email.com")
            add_client(cur, "vasya", "pupkin", "notvasya@email.com")
            add_client(cur, "vasya", "nepupkin", "vasya3@email.com")
            update_client(cur, 3, name="nevasya")
            add_phone_number(cur, 79998887761, 1)
            add_phone_number(cur, 79998887762, 1)
            add_phone_number(cur, 79998887763, 2)
            add_phone_number(cur, 79998887764, 4)
            print(find_client_id(cur, email="vasya@email.com"))
            print(find_client_id(cur, phone_number=79998887764))
            print(find_client_id(cur, name="vasya"))
            print(find_client_id(cur, lastname="pupkin"))
            print(find_client_id(cur, name="vasya", lastname="pupkin"))
            print(find_client_id(cur, name="invalid data"))
            delete_phone(cur, phone_number=79998887763)
            delete_client(cur, 1)
            print(find_client_id(cur, email="vasya@email.com"))
