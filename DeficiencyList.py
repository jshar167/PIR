
import time
from dateutil.rrule import rrule, MONTHLY
import datetime
import mysql.connector
import pandas as pd
import os


print("Reading Customers name")
filter = pd.read_excel(r"customer_filter_name.xlsx", engine='openpyxl')

filter_name = tuple(set(filter['Customer_name']))
if len(filter['Customer_name'])==1:
     filter_name = '(%s)' % ', '.join(map(repr, tuple(set(filter['Customer_name']))))
else:
     filter_name = str(tuple(set(filter['Customer_name'])))



print("customers name are-")
print(filter_name)




fst = time.time()

print("setting up connection with MYSQL")
print("Make sure you are connected to VPN")

try:
    connection = mysql.connector.connect(host='prod-rds-bi.xaapbuildings.com',
                                                 user='sandeep.sharma',
                                                 port = '3306',
                                                 password='k3XqezAa')


    cursor = connection.cursor()
    
    df = pd.DataFrame(columns = ['customer_name','building_name','id','device_type','summary_status', 'last_updated_at'])

    a = datetime.date(2019, 1, 1)
    b = datetime.date(2022, 7, 1)
    
    ls = []
    for dt in rrule(MONTHLY, interval = 24, dtstart=a, until=b):
        ls.append(dt.strftime("%Y-%m-%d"))
    ls.append(b.strftime("%Y-%m-%d"))

    for i in range (len (ls)-1):
        print("fetching details for part " +str(i+1))
    
        Query1 = """select distinct c.name as customer_name, 
            b.name as building_name, 
            f.id ,
            f.device_type,
            f.summary_status,
            f.last_updated_at
            from bi.deficiency f, bi.customer c, bi.building b
            where f.tenant_id = c.id
            and f.building_id = b.id
            and c.name in {}
            and f.last_updated_at >= '{}' and f.last_updated_at < '{}' order by id;""".format(filter_name,ls[i], ls[i+1])

        
        st = time.time()
        
        
        cursor = connection.cursor()
        cursor.execute(Query1)
        table_rows = cursor.fetchall()
        
        print(str(i+1) +"part is done")
        et = time.time()
        time_taken = et-st

        df_sub = pd.DataFrame(table_rows)
        
        try:
            df_sub.columns = ['customer_name','building_name','id','device_type','summary_status', 'last_updated_at']
        except:
            df_sub = pd.DataFrame(columns = ['customer_name','building_name','id','device_type','summary_status', 'last_updated_at'])
        
        df = pd.concat([df, df_sub], ignore_index=True)
        
        print("time taken for sub query {} is {} sec".format(i+1, time_taken))
        
        

except mysql.connector.Error as error:
        print("Failed to create table in MySQL: {}".format(error))
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
            
fet = time.time()




final_time_taken = fet - fst


print("total time taken in fetching data is {} sec".format( final_time_taken))




df['last_updated_at'] = pd.to_datetime(df['last_updated_at'], format="%Y-%m-%dT%H:%M:%S")
df['year'] = pd.DatetimeIndex(df['last_updated_at']).year

print("saving the output")

try:
    deficiency_pivot = pd.pivot_table(data=df, index=['customer_name', 'building_name','summary_status', 'device_type'], 
                            values=['id'],
                            columns=['year'],
                            aggfunc='count', 
                            margins=['customer_name', 'building_name','summary_status','device_type'],
                            margins_name='Grand Total',
                            fill_value=0)
except:
    deficiency_pivot =  pd.DataFrame(columns = ['customer_name', 'building_name','summary_status', 'device_type'])




def save_excel_sheet(df, path, sheet_name, index=False):
    # Create file if it does not exist
    if not os.path.exists(path):
        df.to_excel(path, sheet_name=sheet_name, index=index)

    # Otherwise, add a sheet. Overwrite if there exists one with the same name.
    else:
        with pd.ExcelWriter(path, engine='openpyxl', if_sheet_exists='replace', mode='a') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)

            
path = r"final_209.xlsx"
df = deficiency_pivot
sheet_name = "DeficiencyList_pivot"

save_excel_sheet(df, path, sheet_name, index = True)

print("Results are saved at " +path +" with sheet name " +sheet_name)



