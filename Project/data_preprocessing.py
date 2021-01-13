import pandas


def clean_age_groups(row):
    return row if row in ['<18', '18-24', '25-44', '45-64', '65+', 'UNKNOWN'] else 'UNKNOWN'


def assign_age_groups(row):
    if row < 0:
        return 'UNKNOWN'
    if row < 18:
        return '<18'
    elif row < 25:
        return '18-24'
    elif row < 45:
        return '25-44'
    elif row < 65:
        return '45-64'
    else:
        return '65+'


def assign_race(row):
    if row == 'W':
        return 'WHITE'
    elif row == 'B':
        return 'BLACK'
    elif row == 'H':
        return 'HISPANIC'
    elif row == 'I':
        return 'AMERICAN INDIAN/ALASKAN NATIVE'
    elif row == 'O':
        return 'OTHER'
    elif row in ['A', 'C', 'D', 'F', 'G', 'J', 'K', 'L', 'P', 'S', 'U', 'V', 'Z']:
        return 'ASIAN / PACIFIC ISLANDER'
    else:
        return 'UNKNOWN'


def merge_hispanic(row):
    if row in ['WHITE HISPANIC', 'BLACK HISPANIC']:
        return 'HISPANIC'
    else:
        return row


# NEW YORK ####################################################################
dfny = pandas.read_csv(
    'data/NYPD_Complaint_Data_Historic.csv',
    usecols=['CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'PD_CD', 'PD_DESC', 'BORO_NM', 'LOC_OF_OCCUR_DESC', 'PREM_TYP_DESC',
             'SUSP_AGE_GROUP', 'SUSP_RACE', 'SUSP_SEX', 'VIC_AGE_GROUP', 'VIC_RACE', 'VIC_SEX']
)
# Date = CMPLNT_FR_DT, CMPLNT_FR_TM
dfny['Date'] = dfny['CMPLNT_FR_DT'] + ' ' + dfny['CMPLNT_FR_TM']
dfny['Date'] = pandas.to_datetime(dfny['Date'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
dfny = dfny[dfny['Date'].notna()]
# Crime Code = PD_CD
dfny = dfny[dfny['PD_CD'].notna()]
dfny['PD_CD'] = dfny['PD_CD'].astype(int)
# Location Type = LOC_OF_OCCUR_DESC, PREM_TYP_DESC
dfny['LOC_OF_OCCUR_DESC'].fillna('', inplace=True)
dfny['Location Type'] = dfny['LOC_OF_OCCUR_DESC'] + ' ' + dfny['PREM_TYP_DESC']
dfny = dfny[dfny['Location Type'].notna()]
# Suspect/Victim Sex = SUSP_SEX, VIC_SEX
dfny['SUSP_SEX'].fillna('U', inplace=True)
dfny['VIC_SEX'].fillna('U', inplace=True)
dfny = dfny[~dfny['VIC_SEX'].isin(['E', 'D'])]
# Suspect/Victim Age Group = SUSP_AGE_GROUP, VIC_AGE_GROUP
dfny['SUSP_AGE_GROUP'] = dfny['SUSP_AGE_GROUP'].apply(clean_age_groups)
dfny['VIC_AGE_GROUP'] = dfny['VIC_AGE_GROUP'].apply(clean_age_groups)
# Suspect/Victim Race = SUSP_RACE, VIC_RACE
dfny['SUSP_RACE'].fillna('UNKNOWN', inplace=True)
dfny['VIC_RACE'].fillna('UNKNOWN', inplace=True)
dfny['SUSP_RACE'] = dfny['SUSP_RACE'].apply(merge_hispanic)
dfny['VIC_RACE'] = dfny['VIC_RACE'].apply(merge_hispanic)
#
dfny.rename({'PD_CD': 'Crime Code',
             'PD_DESC': 'Crime Description',
             'SUSP_AGE_GROUP': 'Suspect Age Group',
             'SUSP_RACE': 'Suspect Race',
             'SUSP_SEX': 'Suspect Sex',
             'VIC_AGE_GROUP': 'Victim Age Group',
             'VIC_RACE': 'Victim Race',
             'VIC_SEX': 'Victim Sex',
             'BORO_NM': 'Area'}, axis=1, inplace=True)
dfny = dfny[['Date', 'Crime Code', 'Crime Description', 'Suspect Age Group', 'Suspect Sex', 'Suspect Race',
             'Victim Age Group', 'Victim Sex', 'Victim Race', 'Location Type', 'Area']]
print('\tNY')
print(dfny.iloc[:, :].sample(5).to_string())

# CHICAGO #####################################################################
dfch = pandas.read_csv(
    'data/Crimes_-_2001_to_Present.csv',
    usecols=['Date', 'Primary Type', 'Description', 'Location Description', 'FBI Code']
)
# Date = Date
dfch['Date'] = pandas.to_datetime(dfch['Date'], format='%m/%d/%Y %H:%M:%S %p')
# Location Type = Location Description
dfch = dfch[dfch['Location Description'].notna()]
#
dfch.rename({'Description': 'Crime Details',
             'Primary Type': 'Crime Description',
             'FBI Code': 'Crime Code',
             'Location Description': 'Location Type'}, axis=1, inplace=True)
dfch = dfch[['Date', 'Crime Code', 'Crime Description', 'Crime Details', 'Location Type']]
print('\tCH')
print(dfch.iloc[:, :].sample(5).to_string())

# LOS ANGELES #################################################################
dfla = pandas.read_csv(
    'data/Crime_Data_from_2010_to_2019.csv',
    usecols=['DATE OCC', 'AREA NAME', 'Crm Cd', 'Crm Cd Desc', 'Vict Age', 'Vict Sex', 'Vict Descent', 'Premis Desc']
)
# Date = DATE OCC
dfla['DATE OCC'] = pandas.to_datetime(dfla['DATE OCC'], format='%m/%d/%Y %H:%M:%S %p')
# Victim Age Group = Vict Age
dfla['Vict Age'] = dfla['Vict Age'].apply(assign_age_groups)
# Victim Sex = Vict Sex
dfla['Vict Sex'] = dfla['Vict Sex'].apply(lambda row: 'U' if row not in ['M', 'F'] else row)
# Victim Race = Vict Descent
dfla['Vict Descent'].fillna('X', inplace=True)
dfla['Vict Descent'].replace('-', 'X', inplace=True)
dfla['Vict Descent'] = dfla['Vict Descent'].apply(assign_race)
# Area = AREA NAME
dfla['AREA NAME'] = dfla['AREA NAME'].apply(lambda row: row.upper())
# Location Type = Premis Desc
dfla = dfla[dfla['Premis Desc'].notna()]
#
dfla.rename({'DATE OCC': 'Date',
             'Crm Cd': 'Crime Code',
             'Crm Cd Desc': 'Crime Description',
             'Vict Age': 'Victim Age Group',
             'Vict Sex': 'Victim Sex',
             'Vict Descent': 'Victim Race',
             'Premis Desc': 'Location Type',
             'AREA NAME': 'Area'}, axis=1, inplace=True)
dfla = dfla[['Date', 'Crime Code', 'Crime Description', 'Victim Age Group', 'Victim Sex', 'Victim Race',
             'Location Type', 'Area']]
print('\tLA')
print(dfla.iloc[:, :].sample(5).to_string())

# SAVE ########################################################################
dfny.to_csv('data/dfny.csv')
dfch.to_csv('data/dfch.csv')
dfla.to_csv('data/dfla.csv')
