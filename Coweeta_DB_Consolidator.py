import pandas as pd
import sqlite3
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')

# Set up database path and name
# To run yourself, only modify lines 10 and 11 to
# define your directory containing the database
main_path = "C:/HydroStuff/Coweeta"
db_name = "coweeta_database.db"
db_path = f"{main_path}/{db_name}"
fig_name = 'Coweeta118Soil_30vs60cm.png' # Name for exported figure
fig_path = f'{main_path}/{fig_name}'

# Connect to the database
conn = sqlite3.connect(f"{db_path}")
cursor = conn.cursor()

# SQL scripts to grab all data from site 118 where values are
# less than 1 and greater than 0.05 (to remove error values)
sdate = "2000-01-01 00:00:00"
scr_s118 = "SELECT * FROM soil_table " \
            "WHERE Site == 'CWT_118' AND mwctop30 < 1 AND mwctop30 > 0.05 " \
           "AND DateTime >= ('2000-01-01 00:00:00') AND DateTime <= ('2007-02-28 23:00:00')"

scr_met = "SELECT * FROM met_table WHERE Date <= ('2007-02-28 23:00:00')"

# Define columns of interest to facilitate dataframe construction
soil_col_names = ["DateTime", "Site", "mwctop30", "mwctop60", "mwcbot30", "mwcbot60"]
met_col_names = ["Date", "Station", "Precip(totalmm)", "Airtemp(meanc)", "SolarRadiationMJm-2", "RelativeHumidity(%)", "U(msec-1)", "VaporPressureDeficit(Pa)"]

# Function to execute the script and set the "DateTime" column to date time format
def sql_executer(script, cnames):
    cursor.execute(script)
    fetched_data = cursor.fetchall()
    df_full = pd.DataFrame(fetched_data, columns = cnames)
    df_full[cnames[0]] = pd.to_datetime(df_full[cnames[0]])
    return df_full

# Call the executer function
soil118_hourly = sql_executer(scr_s118, soil_col_names)
soil118_hourly = soil118_hourly.set_index(['DateTime'])
met118 = sql_executer(scr_met, met_col_names)
met118 = met118.set_index(['Date'])
soil118 = soil118_hourly.resample(rule = "D").mean(numeric_only = True)
print(soil118)

# Take averages of the 30cm and 60cm columns, then remove them
soil118['mwc30av'] = soil118[['mwctop30', 'mwcbot30']].mean(axis=1)
soil118['mwc60av'] = soil118[['mwctop60', 'mwcbot60']].mean(axis=1)
soil118 = soil118.drop(["mwctop30", "mwctop60", "mwcbot30", "mwcbot60"], axis=1)

# Add in pre-created leaf-out function
leaf = pd.read_csv(f'{main_path}/leaf_out_func1.csv', low_memory = False)
leaf["DateTime"] = pd.to_datetime(leaf["DateTime"])
leaf = leaf.set_index('DateTime')

print(leaf)
sleaf = pd.concat([leaf, soil118], axis=1)
print('sleaf: ', sleaf)

combined = pd.concat([sleaf, met118], axis=1)
combined.index.names=['Date']
combined = combined.drop(['U(msec-1)'], axis=1)
combined = combined.dropna()

print(combined)

combined.to_csv(f"{main_path}/data_full.csv", index=True)

print('Complete!')