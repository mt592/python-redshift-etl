'''Run ETL pipeline'''
import sys
from src import extract_data
from src import transform_data
from src import load_data


def main(file):
    path = str(sys.argv[1])
    
    df = extract_data.extract_csv(path)
    df = transform_data.transform_csv(df)
    
    con = load_data.connect_database()
    con.autocommit = True
    cursor = con.cursor()
    load_data.load_csv(df, cursor)
    con.close()

if __name__ == '__main__':
    main(sys.argv[1])