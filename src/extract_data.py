'''Extract raw data from flat file, database, etc. Edit as needed.'''
import pandas as pd

def extract_csv(path):
    data = pd.DataFrame(pd.read_csv(path))
    
    return data