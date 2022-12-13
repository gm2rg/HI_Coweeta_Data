import pandas as pd
from datetime import datetime

# Set up main path and raw meteorological data name
main_path = "C:/HydroStuff/Coweeta"
met_name = "MeteorologicalData_CS01RG06.xls"
met_path = f"{main_path}/{met_name}"

# Start the meteorological data processing
d_start = "2000-01-01" # Beginning of desired record
# Function to read in specific sheets of the single raw data excel file
def excel_import(sheet, columns, date_column):
    met_df = pd.read_excel(met_path, sheet_name = sheet)
    range_mask = (met_df[date_column] >= d_start) # Set up date range to select (rows)
    met_df = met_df.loc[:, columns] # Grab only columns specified
    met_df = met_df.loc[range_mask] # Then, collect only the dates within the mask
    return met_df

# Define column names we are interested in for each sheet
m1c = ["Station", "Date", "Precip (total mm)", "Airtemp (mean c)"]
m2c = ["CS01", "Solar Radiation  MJ m-2 "]
m3c = ["CS01", "Relative Humidity (%)", "U (m sec-1 )", "Vapor Pressure Deficit (Pa)"]

# Three individual dataframes, made by calling the function to get columns/rows from specific sheets
met1 = excel_import("Temp_CWTDB", m1c, "Date")
met2 = excel_import("Radiation", m2c, "CS01")
met3 = excel_import("Hum&VP&U", m3c, "CS01")

# Next, set up the dataframes for concatenation, then combine
met1 = met1.set_index('Date')
met2 = met2.set_index('CS01')
met2.index.name = 'Date'
met3 = met3.set_index('CS01')
met3.index.name = 'Date'

combined_23 = pd.concat([met2, met3], axis = 1)
cfull = pd.concat([met1, combined_23], axis = 1)
print(cfull)

# Export the cleaned meteorological data to a csv file
met_name = "met_table"
cfull.to_csv(f"{main_path}/{met_name}.csv", index=True)

# Start soil moisture processing
# Read in the raw soil data
soil_name = "1013_18_1_1013.txt"
soil_path = f"{main_path}/{soil_name}"
soilm = pd.read_csv(soil_path, sep = "\t", low_memory = False)

# Define columns of interest
scol = ["Site", "Year", "YearDay", "Hour", "mwctop30", "mwctop60", "mwcbot30", "mwcbot60"]
soilm = soilm.loc[:, scol] # Grab only the columns defined above
soilm = soilm.dropna(subset=['mwctop30']) # Remove the null values from the dataframe

# Next, we need to clean the date time information
# The soil data reports the date/time in three separate columns
# Year, day of the year (DoY, value between 1 and 365 or 366), and hour of the day

# We need to comb through the data to remove corrupt data where date
# time information is broken or missing (some date time values are
# reported as decimals like 1.28235 or 5.907236 and not salvageable)

dtcol = [] # Set up the empty list that will contain the new combined date/time column

x = 0
yrange = len(soilm["Year"])
# This creates the combined date/time column by iterating through each row of the soil dataframe
while x < yrange:
    yearcol = soilm["Year"]
    daycol = soilm["YearDay"]
    hcol = soilm["Hour"]
    # Removes all colons from the hour column
    hr = str(hcol.iloc[x])
    hr = hr.replace(":", "")
    # Convert "2400" to "0000", since Python's date time
    # format only accepts 0000 as midnight
    if hr == "2400":
        hr = "0000"
    # Next, extract the year, DoY, and modified hour columns
    # row by row, and add them to a string
    dnum = str(daycol.iloc[x])
    yr = str(yearcol.iloc[x])
    format = f"{dnum}/{yr} {hr}"
    # Convert the string to datetime format
    dt_val1 = datetime.strptime(format, "%j/%Y %H%M")
    dt_val2 = dt_val1.strftime("%Y-%m-%d %H:%M:%S")
    # Add the datetime value to the "dcol" list
    dtcol.append(dt_val2)
    x += 1 # Repeat for all rows

# Add in the new date time column ot the dataframe, set it as index,
# and remove unnecessary year, DoY, and hour columns
soilm.insert(1, "DateTime", dtcol)
soilm["DateTime"] = pd.to_datetime(soilm["DateTime"])
soilm = soilm.set_index("DateTime")
soilm = soilm.drop(["Year", "YearDay", "Hour"], axis=1)

# Finally, export the cleaned soil dataframe to a csv
s_name = "soil_table"
soilm.to_csv(f"{main_path}/{s_name}.csv", index=True)
print("Cleaning complete!")