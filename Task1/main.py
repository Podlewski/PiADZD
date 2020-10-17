import pandas as pd
from timeit import default_timer as timer


filePath = '311_Service_Requests_from_2010_to_Present.csv'

sTimer = timer()
df = pd.read_csv(filePath, usecols=['Agency Name', 'Complaint Type', 'Borough'])
lTimer = timer()

print(f'Loading time:     {lTimer - sTimer:7.2f} s')

complaintType = df['Complaint Type'].value_counts()[:10].index.tolist()
cTimer = timer()

agency = df['Agency Name'].value_counts()[:10].index.tolist()
aTimer = timer()

boroughsComplaintTypes = []
boroughs = df['Borough'].dropna().unique()
for borough in boroughs:
    reducedDF = df[df['Borough'].str.contains(borough, na=False)]
    boroughsComplaintTypes.append((borough, reducedDF['Complaint Type'].value_counts()[:10].index.tolist()))
bTimer = timer()

print('\n--- ANALYSIS TIMES ---')
print(f'Complaint Type:   {cTimer - lTimer:7.2f} s')
print(f'Boroughs:         {bTimer - aTimer:7.2f} s')
print(f'Agency:           {aTimer - cTimer:7.2f} s')

print('\n--- ANALYSIS RESULTS ---')
print(f'Complaint type:   {complaintType}')
print('\nComplaint type by Borough:')
for borough, boroughComplaint in boroughsComplaintTypes:
    borough += ':'
    print(f'   {borough:<18}{boroughComplaint}')
print(f'\nAgency:           {agency}')
