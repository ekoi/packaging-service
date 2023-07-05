import logging
import sqlite3
from sqlite3 import Error

from src.commons import settings


def create_sqlite3_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def select_all_form_metadata(db_file):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, created_time, metadata_id, metadata, list_files FROM form_metadata;")
        data = cursor.fetchall()
        print(data)
        return data  # CREATE JSON


# https://www.beekeeperstudio.io/blog/sqlite-json-with-text
def select_form_metadata(db_file):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    # TODO: Use json functionality - https://www.sqlite.org/json1.html
    # sql = f"SELECT form_metadata.id FROM form_metadata WHERE payload like '%\"target\":%https://archivalbot.data-stations.nl%';"
    sql = f"SELECT json_extract(metadata, '$.id') FROM form_metadata;"
    # sql = f"SELECT id FROM form_metadata WHERE json_extract(payload, '$.target.id') LIKE '%https://archivalbot.data-stations.nl%';"
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        print(data)

        return data


def create_tables(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create the parent table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS form_metadata (
            metadata_id TEXT PRIMARY KEY,
            created_date datetime NOT NULL,
            progress_state_url TEXT, 
            progress_state TEXT,
            pid TEXT,
            urn_nbn TEXT,
            error TEXT     
        )
    ''')

    # Create the children table with a foreign key constraint
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            uploaded_status boolean DEFAULT false,
            metadata_id TEXT NOT NULL,
            FOREIGN KEY (metadata_id) REFERENCES form_metadata (metadata_id)
        )
    ''')

    # Save the changes and close the connection
    conn.commit()
    conn.close()


def insert_record(db_file, metadata_record, file_record):
    # Connect to the SQLite database

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute('INSERT INTO form_metadata(metadata_id, created_date) '
                       'VALUES (?, ?)', metadata_record)

        # Insert records into the children table
        if file_record:
            cursor.executemany('INSERT INTO files (file_name, metadata_id) VALUES (?, ?)', file_record)

        # Save the changes and close the connection
        conn.commit()


def find_record_by_metadata_id(db_file, metadata_id):
    # Connect to the SQLite database
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        # Define the parent's name to search for
        # Query the parent and its children
        cursor.execute('''
            SELECT p.metadata_id, c.file_name, c.uploaded_status
            FROM form_metadata p
            INNER JOIN files c ON p.metadata_id = c.metadata_id
            WHERE p.metadata_id = ?
        ''', (metadata_id,))

        # Fetch all the results
        results = cursor.fetchall()
    return results


def update_file_uploaded_status(db_file, metadata_id, filename):
    logging.debug(f'update_file_uploaded_status for filename: {filename}')
    data = (metadata_id, filename)
    # Connect to the SQLite database
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        # Define the child's current name and the new name

        # Update the child's name
        cursor.execute('UPDATE files SET uploaded_status = true WHERE metadata_id = ? AND file_name = ?', (data))

        # Check the number of rows affected by the update
        num_rows_updated = cursor.rowcount

        # Commit the changes and close the connection
        conn.commit()
        # conn.close()

    # Display the result
    if num_rows_updated == 1:
        logging.debug(f"The uploaded_status of '{filename}' has been updated successfully.")
        return True
    else:
        print(f"update_file_uploaded_status - {filename} - No record found.")
        return False


def update_form_metadata_progress_state_url(db_file, metadata_id, progress_state_url):
    data = (progress_state_url, metadata_id)
    # Connect to the SQLite database
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        # Define the child's current name and the new name

        # Update the child's name
        cursor.execute('UPDATE form_metadata SET progress_state_url = ? WHERE metadata_id = ?',
                       (data))

        # Check the number of rows affected by the update
        num_rows_updated = cursor.rowcount

        # Commit the changes and close the connection
        conn.commit()
        # conn.close()

    # Display the result
    if num_rows_updated > 0:
        print(f"The progress_state_url has been updated successfully.")
    else:
        print(f"update_form_metadata_progress_state_url - {metadata_id} - No record found with the given name.")


def update_form_metadata_progress_state(db_file, metadata_id, progress_state):
    data = (progress_state, metadata_id)
    # Connect to the SQLite database
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        # Define the child's current name and the new name

        # Update the child's name
        cursor.execute('UPDATE form_metadata SET progress_state = ? WHERE metadata_id = ?',
                       (data))

        # Check the number of rows affected by the update
        num_rows_updated = cursor.rowcount

        # Commit the changes and close the connection
        conn.commit()
        # conn.close()

    # Display the result
    if num_rows_updated > 0:
        print(f"The progress_state has been updated successfully.")
    else:
        print(f"update_form_metadata_progress_state - {metadata_id} -No record found with the given name.")


def update_form_metadata_error(db_file, metadata_id, error_message):
    data = (error_message, metadata_id)
    # Connect to the SQLite database
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        # Define the child's current name and the new name

        # Update the child's name
        cursor.execute('UPDATE form_metadata SET error = ? WHERE metadata_id = ?',
                       (data))

        # Check the number of rows affected by the update
        num_rows_updated = cursor.rowcount

        # Commit the changes and close the connection
        conn.commit()

    # Display the result
    if num_rows_updated > 0:
        print(f"The progress_state has been updated successfully.")
    else:
        print(f"update_form_metadata_error - {metadata_id} -No record found with the given name.")


def update_form_metadata_pid_urn_nbn(db_file, metadata_id, pid, urn_nbn):
    data = (pid, urn_nbn, metadata_id)
    # Connect to the SQLite database
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        # Define the child's current name and the new name

        # Update the child's name
        cursor.execute('UPDATE form_metadata SET pid = ?, urn_nbn = ? WHERE metadata_id = ?',
                       (data))

        # Check the number of rows affected by the update
        num_rows_updated = cursor.rowcount

        # Commit the changes and close the connection
        conn.commit()

    # Display the result
    if num_rows_updated > 0:
        print(f"The progress_state has been updated successfully.")
    else:
        print(f"update_form_metadata_error - {metadata_id} -No record found with the given name.")


def delete_form_metadata_record(db_file, metadataId):
    # Connect to the SQLite database
    if metadataId == settings.REMOVE_ALL_RECORDS_API_SECRET_KEY:
        delete_all_records_in_all_tables(db_file)
        return -1

    num_rows_deleted = delete_record_by_metadata_id(db_file, metadataId)

    return num_rows_deleted


def delete_all_records_in_all_tables(db_file):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        # Delete all records from the children table
        cursor.execute('DELETE FROM files')

        # Delete all records from the parents table
        cursor.execute('DELETE FROM form_metadata')

        # Commit the changes and close the connection
        conn.commit()
        # conn.close()
    # Display the result
    print("All records have been deleted successfully.")


def delete_record_by_metadata_id(db_file, metadata_id):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute('''
            DELETE FROM files WHERE metadata_id = ?
        ''', (metadata_id,))
        cursor.execute('''
                    DELETE FROM form_metadata WHERE metadata_id = ?
                ''', (metadata_id,))
        # Check the number of rows affected by the deletion
        num_rows_deleted = cursor.rowcount

        # Commit the changes and close the connection
        conn.commit()
        # conn.close()

        # Display the result
        if num_rows_deleted > 0:
            return num_rows_deleted
        else:
            return 0


def find_progress_state_url_by_metadata_id(db_file, metadata_id):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.progress_state_url
            FROM form_metadata p
            WHERE p.metadata_id = ?
        ''', (metadata_id,))

        # Fetch all the results
        results = cursor.fetchall()

    return results


def find_progress_state_by_metadata_id(db_file, metadata_id):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.progress_state, p.pid, p.urn_nbn, p.error
            FROM form_metadata p
            WHERE p.metadata_id = ?
        ''', (metadata_id,))

        # Fetch all the results
        results = cursor.fetchall()

    return results