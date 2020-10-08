from datetime import datetime
from timeit import default_timer as timer
import pandas as pd


filePath = '311_Service_Requests_from_2010_to_Present.csv'

current_time = datetime.now().strftime("%H:%M:%S")
print(f'Current Time: {current_time}')
sTimer = timer()

df = pd.read_csv(filePath, usecols=['Agency', 'Complaint Type', 'Borough'])
lTimer = timer()

print('\n\nANALYSIS TIME:\n')
print(f'Loading time:  {round((lTimer - sTimer), 5)} s')

complaintType = df['Complaint Type'].value_counts()[:1].index.tolist()[0]
cTimer = timer()

agency = df['Agency'].value_counts()[:1].index.tolist()[0]
aTimer = timer()

boroughsComlaintTypes = []
boroughs = df['Borough'].dropna().unique()
for borough in boroughs:
    reducedDF = df[df['Borough'].str.contains(borough, na=False)]
    boroughsComlaintTypes.append((borough, reducedDF['Complaint Type'].value_counts()[:1].index.tolist()[0]))
bTimer = timer()

print(f'Complaint Type: {round((cTimer - lTimer), 5)} s')
print(f'Boroughs:  {round((bTimer - aTimer), 5)} s')
print(f'Agency:    {round((aTimer - cTimer), 5)} s')

print('\n\nANALYSIS RESULTS:\n')

print(f'Complaint type: {complaintType}')
print('Boroughs complaints types:')
for borough, boroughComplaint in boroughsComlaintTypes:
    print(f'   {borough}: {boroughComplaint}')
print(f'Agency: {agency}\n\n')
