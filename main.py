import os

import pandas as pd
from sqlalchemy import create_engine, text

if __name__ == '__main__':
    csv_file = './asndata/GeoLite2-ASN-Blocks-IPv4.csv'
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"The file {csv_file} does not exist")
         
    db_url = os.getenv('DB_URL')
    if not db_url:
        raise ValueError("No DB_URL environment variable set")
        
    engine = create_engine(db_url)

    with engine.connect() as con:
        con.execute(text("DROP TABLE IF EXISTS ip2asn;"))
        con.execute(text("""
        CREATE TABLE IF NOT EXISTS ip2asn (
        network CIDR PRIMARY KEY,
        asn INTEGER,
        aso TEXT
        );
        """))
        con.commit()
        print('ip2asn table re-created')

        df = pd.read_csv(csv_file, sep=',', header=0, names=['network', 'asn', 'aso'], low_memory=False)
        df.to_sql('ip2asn', con=con, if_exists='append', index=False)
        
    print('ip2asn table populated')
