import psycopg2
import util


def create_tables():
    # db_name, table_name, frequency, inc_col, no_of_record, inc_col_state, load_type, load_timestamp, total_exe_time_sec, state_of_run
    commands = [
        """
        CREATE TABLE ingestion_history (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            frequency VARCHAR(255) NOT NULL,
            db_name VARCHAR(255) NOT NULL,
            load_timestamp TIMESTAMP NOT NULL,
            total_exe_time_sec INTEGER NOT NULL,
            state_of_run BOOLEAN DEFAULT FALSE,
            no_of_record INTEGER NOT NULL,
            load_type VARCHAR(10) NOT NULL,
            inc_state VARCHAR(255),
            inc_col VARCHAR(255)
        )
        """]
    conn = None
    try:
        conn = util.get_metadata_conn()
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
        print("Metadata Table Created!")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
