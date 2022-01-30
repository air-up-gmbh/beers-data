import psycopg2
import util


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = [
        """
        CREATE TABLE beer (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            tagline VARCHAR(255),
            first_brewed VARCHAR(10),
            abv FLOAT,
            yeast VARCHAR(255) 
        )
        """,
        """
        CREATE TABLE ingredients_malt (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            amount_unit VARCHAR(10),
            amount_value FLOAT,
            beer_id INTEGER REFERENCES beer (id)
            )
        """,
        """
        CREATE TABLE ingredients_hops (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            add VARCHAR(50),
            attribute VARCHAR(50),
            amount_unit VARCHAR(10),
            amount_value FLOAT,
            beer_id INTEGER REFERENCES beer (id)
            )
        """
    ]
    conn = None
    try:
        conn = util.get_postgres_conn()
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
        print("Table Created!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
