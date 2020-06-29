'''Transform data as desired. Edit as needed.'''

def transform_csv(data):
    data['column1'] = data['column1'].str.upper()
    
    return data