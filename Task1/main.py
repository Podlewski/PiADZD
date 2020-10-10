from datetime import datetime
from timeit import default_timer as timer
import pandas as pd

filePath = 'V:/311_Service_Requests_from_2010_to_Present-cut.csv'

current_time = datetime.now().strftime("%H:%M:%S")
print(f'Current Time:\t{current_time}')
sTimer = timer()

df = pd.read_csv(filePath, usecols=['Agency', 'Complaint Type', 'Borough'])
lTimer = timer()

complaintType = df['Complaint Type'].value_counts()[:1].index.tolist()[0]
cTimer = timer()

agency = df['Agency'].value_counts()[:1].index.tolist()[0]
aTimer = timer()

boroughsComplaintTypes = []
boroughs = df['Borough'].dropna().unique()
for borough in boroughs:
    reducedDF = df[df['Borough'].str.contains(borough, na=False)]
    boroughsComplaintTypes.append((borough, reducedDF['Complaint Type'].value_counts()[:1].index.tolist()[0]))
bTimer = timer()

print('\nANALYSIS TIMES -------------------------')
print(f'Loading time:  {lTimer - sTimer:10.3f} s')
print(f'Complaint Type:{cTimer - lTimer:10.3f} s')
print(f'Boroughs:      {bTimer - aTimer:10.3f} s')
print(f'Agency:        {aTimer - cTimer:10.3f} s')

print('\nANALYSIS RESULTS -----------------------')
print(f'Complaint type:\n\t{complaintType}')
print('Complaints type by Borough:')
for borough, boroughComplaint in boroughsComplaintTypes:
    print(f'\t{borough:<15}:{boroughComplaint:>20}')
print(f'Agency:\n\t{agency}')
