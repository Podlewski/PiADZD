import pandas as pd


def print_unique(name, data, columns):
    print(f'\n\n=============== {name}')
    for column in columns:
        unique = data[column].unique()
        print(f'\n---> {column} ({len(unique)}):')
        print(unique)


name = 'Chicago'
columns = ['IUCR', 'Primary Type', 'Description', 'Location Description', 'FBI Code',
           'Arrest', 'Beat', 'District', 'Ward', 'Community Area']
data = pd.read_csv('Crimes_-_2001_to_Present.csv', usecols=columns)
print_unique(name, data, columns)

name = 'LA'
columns = ['Crm Cd', 'Crm Cd Desc', 'Vict Descent', 'Premis Cd',
           'Premis Desc', 'Status', 'Status Desc' ]
data = pd.read_csv('Crime_Data_from_2010_to_2019.csv', usecols=columns)
data['Crm Cd Desc'] = data['Crm Cd'].astype(str) + ' ==> ' + data['Crm Cd Desc']
print_unique(name, data, columns)

name = 'NY'
columns = ['KY_CD', 'OFNS_DESC', 'CRM_ATPT_CPTD_CD', 'LOC_OF_OCCUR_DESC',
           'PREM_TYP_DESC', 'SUSP_RACE', 'SUSP_AGE_GROUP',
           'VIC_RACE', 'VIC_AGE_GROUP', 'SUSP_SEX', 'VIC_SEX' ]
data = pd.read_csv('NYPD_Complaint_Data_Historic.csv', usecols=columns)
data['OFNS_DESC'] = data['KY_CD'].astype(str) + ' ==> ' + data['OFNS_DESC']
print_unique(name, data, columns)
