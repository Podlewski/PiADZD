import pandas

dfny = pandas.read_csv(
    'data/NYPD_Complaint_Data_Historic.csv',
    usecols=['CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'PD_CD', 'PD_DESC', 'BORO_NM', 'LOC_OF_OCCUR_DESC', 'PREM_TYP_DESC',
             'SUSP_AGE_GROUP', 'SUSP_RACE', 'SUSP_SEX', 'VIC_AGE_GROUP', 'VIC_RACE', 'VIC_SEX']
)
dfny['Date'] = dfny['CMPLNT_FR_DT'] + ' ' + dfny['CMPLNT_FR_TM']
dfny['Date'] = pandas.to_datetime(dfny['Date'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
dfny = dfny[dfny['Date'].notna()]
dfny['LOC_OF_OCCUR_DESC'].fillna('', inplace=True)
dfny['Location Type'] = (dfny['LOC_OF_OCCUR_DESC']) + ' ' + dfny['PREM_TYP_DESC']
dfny.drop(['CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'LOC_OF_OCCUR_DESC', 'PREM_TYP_DESC'], axis=1, inplace=True)
dfny = dfny[['Date'] + [col for col in dfny.columns if col != 'Date']]
dfny.rename({'PD_CD': 'Crime Code',
             'PD_DESC': 'Crime Description',
             'SUSP_AGE_GROUP': 'Suspect Age Group',
             'SUSP_RACE': 'Suspect Race',
             'SUSP_SEX': 'Suspect Sex',
             'VIC_AGE_GROUP': 'Victim Age Group',
             'VIC_RACE': 'Victim Race',
             'VIC_SEX': 'Victim Sex'}, axis=1, inplace=True)
print('\tNY')
print(dfny.iloc[:, :].sample(10).to_string())

dfch = pandas.read_csv(
    'data/Crimes_-_2001_to_Present.csv',
    usecols=['Date', 'Primary Type', 'Description', 'Location Description', 'FBI Code']
)
dfch['Date'] = pandas.to_datetime(dfch['Date'], format='%m/%d/%Y %H:%M:%S %p')
dfch.rename({'Description': 'Crime Description',
             'FBI Code': 'Crime Code'}, axis=1, inplace=True)
print('\tCH')
print(dfch.iloc[:, :].sample(10).to_string())

dfla = pandas.read_csv(
    'data/Crime_Data_from_2010_to_2019.csv',
    usecols=['DATE OCC', 'AREA ', 'AREA NAME', 'Rpt Dist No', 'Crm Cd', 'Crm Cd Desc', 'Mocodes',
             'Vict Age', 'Vict Sex', 'Vict Descent', 'Premis Cd', 'Status', 'Status Desc']
)
dfla['DATE OCC'] = pandas.to_datetime(dfla['DATE OCC'], format='%m/%d/%Y %H:%M:%S %p')
dfla['Vict Sex'] = dfla['Vict Sex'].apply(lambda row: 'X' if row not in ['M', 'F', 'X'] else row)
dfla['Vict Descent'].fillna('X', inplace=True)
dfla['Vict Descent'].replace('-', 'X', inplace=True)
dfla.rename({'DATE OCC': 'Date',
             'AREA ': 'AREA',
             'Crm Cd': 'Crime Code',
             'Crm Cd Desc': 'Crime Description',
             'Vict Age': 'Victim Age Group',
             'Vict Sex': 'Victim Sex',
             'Vict Descent': 'Victim Race'}, axis=1, inplace=True)
print('\tLA')
print(dfla.iloc[:, :].sample(10).to_string())
