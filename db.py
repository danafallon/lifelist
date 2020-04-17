import csv
import sqlite3

from dateutil.parser import parse as parse_date


DATABASE = 'lifelist.db'
DATA_FILE = 'lifelist-2020-04-12.csv'


def create_sightings_table(cursor):
    cursor.execute("DROP TABLE IF EXISTS sightings")
    cursor.execute("""
        CREATE TABLE sightings (
            id integer PRIMARY KEY,
            bird_name text NOT NULL,
            genus text,
            species text,
            site text,
            date_first_seen text,
            city text,
            county text,
            state text,
            country text,
            continent text,
            comments text
        )
    """)


def process_text_field(row, field_name):
    value = row[field_name]
    if value:
        value = value.replace("'", "''")

    return value


def ingest(filepath, cursor):
    with open(filepath, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # convert date first seen to isoformat
            date_first_seen_raw = row['Date first seen']
            date_first_seen = parse_date(date_first_seen_raw)
            date_first_seen = date_first_seen.isoformat(sep=' ')
            # pull text fields
            bird_name = process_text_field(row, 'Name of Bird')
            genus = process_text_field(row, 'Genus')
            species = process_text_field(row, 'Species')
            site = process_text_field(row, 'Site')
            city = process_text_field(row, 'City (optional)')
            county = process_text_field(row, 'County (optional)')
            state = process_text_field(row, 'State')
            country = process_text_field(row, 'Country ')
            continent = process_text_field(row, 'Continent')
            comments = process_text_field(row, 'Comments')

            insert_stmt = f"""
                INSERT INTO sightings
                    (bird_name, genus, species, site, date_first_seen,
                    city, county, state, country, continent, comments)
                VALUES
                    ('{bird_name}', '{genus}', '{species}', '{site}', '{date_first_seen}',
                    '{city}', '{county}', '{state}', '{country}', '{continent}', '{comments}')
            """
            print(insert_stmt)
            cursor.execute(insert_stmt)


def set_up_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    create_sightings_table(cursor)
    print('created sightings table')
    print('ingesting data...')
    ingest(DATA_FILE, cursor)
    conn.commit()
    conn.close()
    print('finished!')


if __name__ == '__main__':
    set_up_db()
