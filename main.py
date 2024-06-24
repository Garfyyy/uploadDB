import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

def uploaddf(df: pd.DataFrame, con: Connection, table_name: str, columns: str):
    con.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
    con.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"))
    con.commit()
    print(f"{table_name} table re-created")

    df.to_sql(table_name, con=con, if_exists='append', index=False)
    print(f'{table_name} table populated')

def ip2asn_db(con: Connection):
    ip2asn_csv = './data/GeoLite2-ASN-Blocks-IPv4.csv'
    if not os.path.exists(ip2asn_csv):
        raise FileNotFoundError("ASN file is missing")

    ip2asn_df = pd.read_csv(ip2asn_csv, sep=',', header=0, names=['network', 'asn', 'aso'], low_memory=False)
    
    table_name = 'ip2asn'
    columns = 'network CIDR PRIMARY KEY, asn INTEGER, aso TEXT'
    uploaddf(ip2asn_df, con, table_name, columns)

def ip2country_db(con: Connection):
    country_ipv4_path = './data/GeoLite2-Country-Blocks-IPv4.csv'
    country_ipv4_locations_path = './data/GeoLite2-Country-Locations-en.csv'
    if not os.path.exists(country_ipv4_path) or not os.path.exists(country_ipv4_locations_path):
        raise FileNotFoundError("Country files are missing")

    country_ipv4_df = pd.read_csv(country_ipv4_path, usecols=['network', 'geoname_id', 'registered_country_geoname_id'], low_memory=False)
    country_loc_df = pd.read_csv(country_ipv4_locations_path, usecols=['geoname_id', 'country_iso_code', 'country_name'], low_memory=False).rename(columns={'country_iso_code': 'country_code'})

    country_ipv4_df['geoname_id'] = country_ipv4_df['geoname_id'].fillna(country_ipv4_df['registered_country_geoname_id'])
    country_ipv4_df.drop(columns=['registered_country_geoname_id'], inplace=True)

    # network	country_code	country_name
    new_df = pd.merge(country_ipv4_df, country_loc_df, on='geoname_id', how='inner').drop(columns=['geoname_id'])

    table_name = 'ip2country'
    columns = 'network CIDR PRIMARY KEY, country_code TEXT, country_name TEXT'

    uploaddf(new_df, con, table_name, columns)

if __name__ == '__main__':

    db_url = os.getenv('DB_URL')
    if not db_url:
        raise ValueError("No DB_URL environment variable set")

    engine = create_engine(db_url)

    with engine.connect() as con:
        ip2asn_db(con)
        ip2country_db(con)
