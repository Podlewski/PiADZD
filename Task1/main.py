from datetime import datetime
from timeit import default_timer as timer
import pandas as pd


filePath = '311_Service_Requests_from_2010_to_Present.csv'

current_time = datetime.now().strftime("%H:%M:%S")
print(f'Current Time: {current_time}')
start = timer()

df = pd.read_csv(filePath, usecols=['Agency', 'Complaint Type', 'City'])

middle = timer()
print(f'\nLoading time:  {round((middle - start), 5)} s')

complaintType = df['Complaint Type'].value_counts()[:1].index.tolist()[0]

citiesComlaintTypes = []
cities = df['City'].dropna().unique()
# for city in cities:
#     cityDF = df[df['City'].str.contains(city, na=False)]
#     citiesComlaintTypes.append((city, cityDF['Complaint Type'].value_counts()[:1].index.tolist()[0]))

agency = df['Agency'].value_counts()[:1].index.tolist()[0]

end = timer()
print(f'Analysis time: {round((end - middle), 5)} s\n')

print(f'Complaint Type: {complaintType}')
# print('Cities:')
# for city, cityComplaintType in citiesComlaintTypes:
#     print(f'  {city}: {cityComplaintType}')
print(f'Agency: {agency}')


