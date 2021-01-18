import pandas
from datetime import datetime
from preprocessing_utils import simplify_arrest_status
from crime_codes_la import assign_nibris_code_la
from crime_codes_ny import assign_nibris_code_ny
from preprocessing_utils import *


def preporcess_chicago():
    df = pandas.read_csv(
        'Crimes_-_2001_to_Present.csv',
        usecols=['Date', 'Description', 'Location Description', 'FBI Code',
                 'IUCR', 'Arrest', 'District'])

    df = df[df['FBI Code'].notna()]
    df['Crime Code'] = df['FBI Code'].apply(reduce_nibris_code_chichago)
    df['Crime Category'] = df['Crime Code'].apply(translate_nibric_codes)
    
    df['Date'] = pandas.to_datetime(df['Date'], format='%m/%d/%Y %H:%M:%S %p')
    df = df[df['Date'].notna()]
    df = df[df['Date'].dt.year > 2009]

    df['Location Description'].fillna('UNKOWN', inplace=True) 

    df.rename({'Description': 'Crime Description',
               'IUCR': 'Local Crime Code',
               'Location Description': 'Location Type',
               'District': 'Area'}, axis=1, inplace=True)
    df = df[['Date', 'Crime Code', 'Crime Category', 'Local Crime Code',
             'Crime Description', 'Arrest', 'Location Type', 'Area']]
    
    df['Location Type'] = df['Location Type'].apply(cut_location_types_ch)
    print('CHICAGO:')
    print(df.iloc[:, :].sample(15))
    df.to_csv('dfch.csv', index=False) 


def preporcess_los_angeles():
    df = pandas.read_csv(
        'Crime_Data_from_2010_to_2019.csv',
        usecols=['DATE OCC', 'TIME OCC', 'AREA NAME', 'Crm Cd', 'Crm Cd Desc',
                 'Vict Age', 'Vict Sex', 'Vict Descent', 'Premis Desc',
                 'Status Desc', 'Date Rptd'])

    df = df[df['Crm Cd'].notna()]
    df['Crime Code'] = df['Crm Cd'].apply(assign_nibris_code_la)
    df['Crime Category'] = df['Crime Code'].apply(translate_nibric_codes)
    
    df['Time'] = df['TIME OCC'] = df['TIME OCC'].apply(lambda row: str(row).zfill(4)[:2] + ':' + str(row).zfill(4)[2:])
    df['Date'] = df['DATE OCC'].apply(lambda row: str(row).split(' ', 1)[0])
    df['Date'] = df['Date']  + ' ' + df['Time']
    df['Date'] = pandas.to_datetime(df['Date'], format='%m/%d/%Y %H:%M', errors='coerce')
    df = df[df['Date'].notna()]

    df['Date Rptd'] = df['Date Rptd'].apply(lambda row: str(row).split(' ', 1)[0])
    df['Date Reported'] = pandas.to_datetime(df['Date Rptd'], format='%m/%d/%Y')
    
    df['Victim Age Group'] = df['Vict Age'].apply(assign_age_groups)
    
    df['Victim Sex'] = df['Vict Sex'].apply(lambda row: 'U' if row not in ['M', 'F'] else row)

    df['Vict Descent'].fillna('UNKOWN', inplace=True)
    df['Vict Descent'].replace('-', 'UNKNOWN', inplace=True)
    df['Victim Race'] = df['Vict Descent'].apply(assign_race)
    
    df['Area'] = df['AREA NAME'].apply(lambda row: row.upper())
    
    df['Premis Desc'].fillna('UNKOWN', inplace=True)
    
    df['Arrest'] = df['Status Desc'].apply(simplify_arrest_status)
    
    df.rename({'Crm Cd': 'Local Crime Code',
               'Crm Cd Desc': 'Crime Description',
               'Premis Desc': 'Location Type'}, axis=1, inplace=True)
    df = df[['Date', 'Date Reported', 'Crime Code', 'Crime Category',
             'Local Crime Code', 'Crime Description', 'Victim Age Group',
             'Victim Sex', 'Victim Race', 'Arrest', 'Location Type', 'Area']]
    
    df['Location Type'] = df['Location Type'].apply(cut_location_types_la)
    df['Crime Description'] = df['Crime Description'].apply(cut_crimes_types_la)
    print('\nLOS ANGELES:')
    print(df.iloc[:, :].sample(15))
    df.to_csv('dfla.csv', index=False) 


def preprocess_new_york():
    df = pandas.read_csv(
        'NYPD_Complaint_Data_Historic.csv',
        usecols=['CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'KY_CD', 'OFNS_DESC', 'BORO_NM',
                 'PREM_TYP_DESC', 'SUSP_AGE_GROUP', 'SUSP_RACE', 'RPT_DT',
                 'SUSP_SEX', 'VIC_AGE_GROUP', 'VIC_RACE', 'VIC_SEX'])
    
    df['Date'] = df['CMPLNT_FR_DT'] + ' ' + df['CMPLNT_FR_TM']
    df['Date'] = pandas.to_datetime(df['Date'], format='%m/%d/%Y %H:%M:%S', errors='coerce')
    df = df[df['Date'].notna()]
    df = df[df['Date'].dt.year > 2009]

    df['Date Reported'] = pandas.to_datetime(df['RPT_DT'], format='%m/%d/%Y', errors='coerce')
    
    df = df[df['KY_CD'].notna()]
    df['KY_CD'] = df['KY_CD'].astype(int)

    df['Crime Code'] = df['KY_CD'].apply(assign_nibris_code_ny)
    df['Crime Category'] = df['Crime Code'].apply(translate_nibric_codes)
    
    df['PREM_TYP_DESC'].fillna('UNKOWN', inplace=True) 
    
    df['SUSP_SEX'].fillna('U', inplace=True)
    df['VIC_SEX'].fillna('U', inplace=True)
    df = df[~df['VIC_SEX'].isin(['E', 'D'])]
    
    df['SUSP_AGE_GROUP'] = df['SUSP_AGE_GROUP'].apply(clean_age_groups)
    df['VIC_AGE_GROUP'] = df['VIC_AGE_GROUP'].apply(clean_age_groups)
    
    df['SUSP_RACE'].fillna('UNKNOWN', inplace=True)
    df['VIC_RACE'].fillna('UNKNOWN', inplace=True)
    df['SUSP_RACE'] = df['SUSP_RACE'].apply(merge_hispanic)
    df['VIC_RACE'] = df['VIC_RACE'].apply(merge_hispanic)
    
    df.rename({'KY_CD': 'Local Crime Code',
               'OFNS_DESC': 'Crime Description',
               'SUSP_AGE_GROUP': 'Suspect Age Group',
               'SUSP_RACE': 'Suspect Race',
               'SUSP_SEX': 'Suspect Sex',
               'VIC_AGE_GROUP': 'Victim Age Group',
               'VIC_RACE': 'Victim Race',
               'VIC_SEX': 'Victim Sex',
               'PREM_TYP_DESC': 'Location Type',
               'BORO_NM': 'Area'}, axis=1, inplace=True)
    df = df[['Date', 'Date Reported', 'Crime Code', 'Crime Category',
             'Local Crime Code', 'Crime Description', 'Suspect Age Group',
             'Suspect Sex', 'Suspect Race', 'Victim Age Group', 'Victim Sex',
             'Victim Race', 'Location Type', 'Area']]
    
    df['Location Type'] = df['Location Type'].apply(cut_location_types_ny)
    print('\nNew York')
    print(df.iloc[:, :].sample(15))
    df.to_csv('dfny.csv', index=False) 


def main():
    preporcess_chicago()
    preporcess_los_angeles()
    preprocess_new_york()


if __name__ == "__main__":
    main()
