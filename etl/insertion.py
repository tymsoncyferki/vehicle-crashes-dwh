import pyodbc
import pandas as pd

from config import Config


def load_data_to_dwh(table, table_name, skip_duplicates=True):

    conn = connect_to_db()
    if conn is None:
        return False

    cursor = conn.cursor()
    query = generate_insertion_query(table_name, table.columns, skip_duplicates=skip_duplicates)

    for index, row in table.iterrows():
        try:
            values = generate_cursor_values(row, table.columns)
            cursor.execute(query, values)
        except Exception as e:
            print("An error occurred:", e)
            print(row)
            cursor.close()
            conn.close()
            return False

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{table_name}: data loaded succesfully")
    return True


def connect_to_db():
    connection_string = f"""
        DRIVER={{{Config.DRIVER_NAME}}};
        SERVER={{{Config.SERVER_NAME}}};
        DATABASE={{{Config.DATABASE_NAME}}};
        Trust_Connection=yes;
        uid={{{Config.DWH_USER}}};
        pwd={{{Config.DWH_PASSWORD}}};
    """

    try:
        conn = pyodbc.connect(connection_string)
        print(f'Connection to {Config.DATABASE_NAME}@{Config.SERVER_NAME} succesful:', conn)
    except Exception as e:
        print(f'Could not connect to {Config.DATABASE_NAME}@{Config.SERVER_NAME}:', e)
        return None

    return conn


def generate_insertion_query(table_name, columns, skip_duplicates=True):
    column_list = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_statement = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders});"

    if skip_duplicates:
        query = f"""
        BEGIN TRY
            {insert_statement}
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() = 2601 OR ERROR_NUMBER() = 2627
            BEGIN
                PRINT 'Duplicate key, skipping row'
            END
            ELSE
            BEGIN
                THROW;
            END
        END CATCH
        """
    else:
        query = insert_statement
    return query


def generate_cursor_values(row, columns):
    values = [row[col] for col in columns]
    return values


def check_last_update():
    conn = connect_to_db()
    if conn is None:
        pass

    query = """ select top 1 EndDate from Metadata
            order by LastUpdate DESC """

    cursor = conn.cursor()
    cursor.execute(query)

    rows = cursor.fetchall()
    end_date = rows[0][0]

    cursor.close()
    conn.close()

    return end_date
