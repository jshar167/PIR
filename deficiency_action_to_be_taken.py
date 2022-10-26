print("starting script")


import time
from dateutil.rrule import rrule, MONTHLY
import datetime
import mysql.connector
import pandas as pd
import numpy as np
import os


print("reading customer names")


filter = pd.read_excel(r"customer_filter_name.xlsx", engine='openpyxl')





filter_name = tuple(set(filter['Customer_name']))
if len(filter['Customer_name'])==1:
     filter_name = '(%s)' % ', '.join(map(repr, tuple(set(filter['Customer_name']))))
else:
     filter_name = str(tuple(set(filter['Customer_name'])))


print("name of customers--" )
print(filter_name)
print("setting connection")


fst = time.time()

try:
    connection = mysql.connector.connect(host='prod-rds-bi.xaapbuildings.com',
                                                 user='sandeep.sharma',
                                                 port = '3306',
                                                 password='k3XqezAa')


    cursor = connection.cursor()
    
    df = pd.DataFrame(columns = ['customer_name','building_name','device_id','device_type',
                                 'status','failed_reason', 'device_manufacturer', 'device_model',
                                 'inspection_type','end_date','question', 'answer'])

    a = datetime.date(2019, 1, 1)
    b = datetime.date(2022, 7, 1)
    
    ls = []
    for dt in rrule(MONTHLY, interval = 24, dtstart=a, until=b):
        ls.append(dt.strftime("%Y-%m-%d"))

    for i in range (len (ls)-1):
        print("fetching details for part" +str(i+1))
    
        mySql_Create_Table_Query = """select distinct c.name as customer_name,
            b.name as building_name,
            d.device_id as device_id,
            d.type as device_type,
            d.status as status,
            d.failed_reason,
            d.manufacturer as device_manufacturer,
            d.model as device_model,
            i.inspection_type,
            i.end_date,
            qa.question as question,
            qa.answer as answer
        from bi.customer c, bi.building b, bi.inspection i, bi.device d, bi.question_answer qa
        where i.tenant_id = c.id
        and i.building_id = b.id
        and i.id = d.inspection_id 
        and d.id = qa.device_id
        and c.name in {}
        and qa.question LIKE '%Date%'
        and i.end_date >= '{}' and i.end_date < '{}' order by device_id;""".format(filter_name, ls[i], ls[i+1])
        
        
        st = time.time()
        #print(st)
        
        connection = mysql.connector.connect(host='prod-rds-bi.xaapbuildings.com',
                                             user='sandeep.sharma',
                                             port = '3306',
                                             password='k3XqezAa')


        cursor = connection.cursor()
        cursor.execute(mySql_Create_Table_Query)
        table_rows = cursor.fetchall()
        
        print(str(i+1) + "is done")
        et = time.time()
        time_taken = et-st

        df_sub = pd.DataFrame(table_rows)
        df_sub.columns = ['customer_name','building_name','device_id','device_type',
                                 'status','failed_reason', 'device_manufacturer', 'device_model',
                                 'inspection_type','end_date','question', 'answer']
        
        df = pd.concat([df, df_sub], ignore_index=True)
        
        print("time taken for sub query {} is {} sec".format(i, time_taken))

        

except mysql.connector.Error as error:
        print("Failed to connect MySQL: {}".format(error))
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
            
fet = time.time()

final_time_taken = fet - fst


print("time taken for all query is {} sec".format( final_time_taken))



df_ques = pd.read_excel(r"Ques_list.xlsx", sheet_name = 'Sheet1', engine='openpyxl')
df_final = df.merge(df_ques, how = 'left', left_on = 'question' , right_on = 'Question')
df_final = df_final.drop(['Question', 'Search Criteria'], axis=1)
df_final['Threshold Value1'] = df_final['Threshold Value']*24*3600
df_final['answer'] = df_final['answer'].fillna('not available')



ls = []
ls1 = []
ls2 = []
n = df_final.shape[0]
       
for i in range(n):
    if (len(df_final['answer'][i]) == 12 or len(df_final['answer'][i]) == 13) and df_final['answer'][i] != 'December 2018':
        j = int(df_final['answer'][i])/1000 + df_final['Threshold Value1'][i]
        k = datetime.datetime.fromtimestamp(np.nan_to_num(j))
        ls.append(k)
        ls1.append(datetime.datetime.fromtimestamp(int(df_final['answer'][i])/1000))
        if (int(df_final['answer'][i])/1000 + df_final['Threshold Value1'][i] < int(time.time())):
            ls2.append('Overdue')
        elif ((int(df_final['answer'][i])/1000 + df_final['Threshold Value1'][i] - int(time.time()))< 180*24*3600):
            ls2.append('Due')
        else:
            ls2.append('Not Due')
     
    else:
        ls.append('Not Calculated')
        ls1.append(df_final['answer'][i])
        ls2.append('Not Due')


df_final['Readable_Date'] = ls1
df_final['Date_Due'] = ls
df_final['Action1'] = ls2

df_final['Criteria Check'] = df_final['Action']
df_final = df_final.drop(['Action'], axis=1)
df_final['Action'] = df_final['Action1']
df_final = df_final.drop(['Action1'], axis=1)

df_final = df_final.drop(['Threshold Value1'], axis=1)



def save_excel_sheet(df, path, sheet_name, index=False):
    # Create file if it does not exist
    if not os.path.exists(path):
        df.to_excel(path, sheet_name=sheet_name, index=index)

    # Otherwise, add a sheet. Overwrite if there exists one with the same name.
    else:
        with pd.ExcelWriter(path, engine='openpyxl', if_sheet_exists='replace', mode='a') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)




print("saving the output")   

path = r"final_209.xlsx"
df = df_final
sheet_name = "Action_needed_to_be_taken"

save_excel_sheet(df, path, sheet_name, index = False)



print("output is saved at -" + path +" with sheet name " +sheet_name)





