print("start script")

from pyArango.connection import *
import pandas as pd
import os


print("reading customer names")


filter = pd.read_excel(r"customer_filter_name.xlsx", engine='openpyxl')


l = list(filter.Customer_name)
print("name of customers--" )
print(l)



conn = Connection(arangoURL = 'http://prod-arangodb-tenant.xaapbuildings.com:8529', 
                  username="xaap_prod_ro", password="30blJnCmGRm@uYGi5n")


print("setting connection")

aql1 = '''FOR t in tenants
FILTER t.name in '''

aql2 = str(l)

aql3 = '''  SORT t.name

FOR device in v_devices
FILTER t._key == device.tenantKey

//customer_id: t._key,
//customer_name: t.name

// optionally, add a specific tenant key here
// FILTER t._key == "116698886"
// optionally, set a limit to reduce load when playing around with this
// LIMIT 0, 5000

// gather the details into an intermediate document
LET sub = MERGE(
   
    // get the customer details or 'unknown' if there is a hanging edge in the graph
    {
        customer_id: t._key,
        customer_name: t.name
    },
   
    // get the parent device details or 'unknown' if there is no parent device
    FIRST(FOR v IN 1..1 INBOUND device._id owns
        FILTER v.type != 'buildings'
        RETURN {
            parent_device_id: v.instanceId,
            parent_device_manufacturer: v.manufacturer,
            parent_device_make: v.make,
            parent_device_model: v.model,
            parent_device_type: v.type,
            parent_device_serial: v.serial}
    ) || {
        'parent_device_id': 'unknown',
        'parent_device_manufacturer': 'unknown',
        'parent_device_make': 'unknown',
        'parent_device_model': 'unknown',
        'parent_device_type': 'unknown',
        'parent_device_serial': 'unknown'
    },
       
    // get the building details or 'unknown' if there is a hanging edge in the graph
    FIRST(FOR v IN 1..2 INBOUND device._id owns
        FILTER v.type == 'buildings'
        RETURN {
            building_id: v._key,
            building_name: v.name
        }
    ) || {
        'building_id': 'unknown',
        'building_name': 'unknown'
    },
   
    // get the device details
    {
        device_id: device.instanceId,
        device_type: device.type,
        device_manufacturer: device.manufacturer,
        device_make: device.make,
        device_model: device.model,
        device_created: device.created
    }
)
// discard any 'unknown' entries since these represent data inconsistencies in (hopefully) non-prod systems
FILTER sub.customer_id != 'unknown' && sub.building_id != 'unknown'
RETURN sub
'''

aql = aql1+aql2+aql3

print("fetching details please wait")

db = conn["tofstenant"]
queryResult = db.AQLQuery(aql, rawResults=True)

df = pd.DataFrame(queryResult)



df1 = df



df1['device_created2'] = pd.to_datetime(df1['device_created'], format="%Y-%m-%dT%H:%M:%S")
df1['year'] = pd.DatetimeIndex(df1['device_created']).year

df1 = df1.drop(['device_created', 'device_created2'], axis=1)




def save_excel_sheet(df, path, sheet_name, index=False):
    # Create file if it does not exist
    if not os.path.exists(path):
        df.to_excel(path, sheet_name=sheet_name, index=index)

    # Otherwise, add a sheet. Overwrite if there exists one with the same name.
    else:
        with pd.ExcelWriter(path, engine='openpyxl', if_sheet_exists='replace', mode='a') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)

            
path = r"final_209.xlsx"

df = df1
#df1['year'] = df1['year'].apply(lambda a: pd.to_datetime(a).date()) 
sheet_name = "BuildingDeviceInventory_raw"

save_excel_sheet(df1, path, sheet_name, index = False)




output = pd.pivot_table(data=df1, index=['customer_name', 'building_name','parent_device_type', 'parent_device_manufacturer',  'parent_device_model',
                                       'device_type','device_manufacturer', 'device_model'], 
                        values=['device_id'],
                        columns=['year'],
                        aggfunc='count',
                        fill_value=''
                    )




print("saving the output")
dataframe = output
sheet_name = "BuildingDeviceInventory_pivot"

save_excel_sheet(dataframe, path, sheet_name,index = True)

print( "output file is saved at " +path + " with sheet name " +sheet_name   )







