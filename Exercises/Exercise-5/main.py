import csv

import psycopg2


def connect():
    try:
        # TODO: use .env file to hide plaintext
        conn = psycopg2.connect(database="exercise_5",
                                host="localhost",
                                user="postgres",
                                password="192003",
                                port="5432")
        print(f"Connected successfully!")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Failed to connect\n{e}")
        return None


# TODO: decouple the main function into smaller functions:
def main():
    connection = connect()
    if connection is None:
        return

    cursor = connection.cursor()

    cursor.execute('''
    create table if not exists accounts
    (
        CUSTOMER_ID int primary key,
        FIRST_NAME  varchar(255) null,
        LAST_NAME   varchar(255) null,
        ADDRESS_1   varchar(255) null,
        ADDRESS_2   varchar(255) null,
        CITY        varchar(255) null,
        STATE       varchar(255) null,
        ZIP_CODE    varchar(255) null,
        JOIN_DATE   date         null
    );
''')

    cursor.execute('''
    create table if not exists products
    (
        PRODUCT_ID          int primary key,
        PRODUCT_CODE        varchar(255) unique null,
        PRODUCT_DESCRIPTION text                null
    );
''')

    cursor.execute('''
    create table if not exists transactions
    (
        TRANSACTION_ID      VARCHAR(255) primary key,
        TRANSACTION_DATE    date         null,
        PRODUCT_ID          int          null,
        PRODUCT_CODE        varchar(255) null,
        PRODUCT_DESCRIPTION text         null,
        QUANTITY            int          null,
        ACCOUNT_ID          int          null,
        
        foreign key (PRODUCT_ID) references products (PRODUCT_ID),
        foreign key (PRODUCT_CODE) references products (PRODUCT_CODE),
        foreign key (ACCOUNT_ID) references accounts (CUSTOMER_ID)
    );
''')

    # TODO: Figure out how to ingest csv to created tables
    csv_location_1 = 'data/accounts.csv'
    csv_location_2 = 'data/products.csv'
    csv_location_3 = 'data/transactions.csv'

    cursor.execute("TRUNCATE table accounts, products, transactions;")
    connection.commit()

    # ingest csv
    with open(csv_location_1) as csv_1:
        reader = csv.reader(csv_1)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO accounts ("
                "customer_id,"
                "first_name,"
                "last_name,"
                "address_1,"
                "address_2,"
                "city,"
                "state,"
                "zip_code,"
                "join_date) VALUES ("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s)", row
            )
        connection.commit()

    with open(csv_location_2) as csv_2:
        reader = csv.reader(csv_2)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO products ("
                "product_id,"
                "product_code,"
                "product_description) VALUES ("
                "%s, %s, %s)", row
            )
        connection.commit()

    with open(csv_location_3) as csv_3:
        reader = csv.reader(csv_3)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO transactions("
                "transaction_id,"
                "transaction_date,"
                "product_id,"
                "product_code,"
                "product_description,"
                "quantity,"
                "account_id) VALUES ("
                "%s, %s, %s, %s, %s, %s, %s)", row
            )
        connection.commit()

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
