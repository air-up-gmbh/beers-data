import psycopg2
import util


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = [
        """
        CREATE TABLE beers (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            tagline VARCHAR(255),
            first_brewed VARCHAR(255),
            abv FLOAT
        )
        """]
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
