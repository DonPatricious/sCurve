import pandas as pd
import numpy as np

#this constant is the project planned cost. This is an actual from the database
project_cost = 1000
section_separator = "----------------------------------------"

#constants. Data should be from a location like a config file, input or database
holidays = ['2026-01-01', '2026-02-25', '2026-04-09', '2026-05-01', '2026-06-12', '2026-08-31', '2026-11-30', '2026-12-25'] #in actual implementation, this should come from a data source 
weekdays_active = "Mon Tue Wed Thu Fri Sat" #workdays including Saturdays #in actual implementation, this should come from a data source
start = '2026-01-01' #time series start date range for the chart generation
end = '2026-01-31' #time series end date range for the chart generation


# "print" statements are for verbose logging purposes. A logging framework can be used instead.

def data_ingestion(file_path): #function to read from a data source(tenant and project specific)

    # Reading from file. This function must be modified to read from the actual database
    print(section_separator)
    print("Data ingestion in progress...")
    print(f"File path: {file_path}")
    df_planned = pd.read_csv(file_path,parse_dates=['Start_date','End_date']) # main line to be modified for database reading
    print("Sample data:")
    print(df_planned.head())
    print("Data ingestion completed.")
    print(section_separator)

    return df_planned

def calculate_activity_weight(dataframe, busDays): #function to calculate activity weight

    print(section_separator)
    print("Calculating activity weights...")
    #computing for "Activity Weight" 
    #formula for activity weight is subtask planned cost / project cost
    #the computed values will be in a new column "Activity Weight"
    dataframe['Activity_Weight'] = dataframe['Planned_Cost'] / project_cost

    print("Adding duration in days...")
    #adding durations in days
    #adding holidays and weekends will affect this computation
    start_d = dataframe["Start_date"].dt.normalize().to_numpy(dtype="datetime64[D]")
    end_d   = dataframe["End_date"].dt.normalize().to_numpy(dtype="datetime64[D]")

    dataframe["Duration_Days"] = np.busday_count(
        start_d,
        end_d + np.timedelta64(1, 'D'),  # add 1 day to include the end date
        busdaycal=busDays   # optional custom calendar
    )

    print("Sample processed data:")
    print(dataframe.head())


    print("Computing average daily accomplishment...")
    #computing the average accomplishment per day
    #formula is Activity weight / duration days
    dataframe['Average_Accomplishement_per_Day'] = dataframe['Activity_Weight'] / dataframe['Duration_Days']

    print("Modifiying the dataframe...")
    #creating a shorter dataframe
    df_processed = dataframe[['SubTaskID','SubTasks','Start_date','End_date','Average_Accomplishement_per_Day']]
    print("Modification completed.")

    print("Activity weight calculation completed.")
    print("Sample processed data:")
    print(df_processed.head())

    return df_processed

def dataframe_assembly(dataframe): #function to assemble the initial dataframe

    print(section_separator)
    print("Assembling the dataframe...")

    #creating an empty dataframe
    df_assembled = pd.DataFrame()

    for index, row in dataframe.iterrows():
        df_container = pd.DataFrame() #temporary dataframe to hold generated dates and accomplishments
        print(f"Processing SubTaskID: {row['SubTaskID']}, SubTask: {row['SubTasks']}")
        print(f"{row['SubTasks']}: Start Date: {row['Start_date'].date()}, End Date: {row['End_date'].date()}, Average Accomplishment per Day: {row['Average_Accomplishement_per_Day']:.4f}")
        
        #generate dates between start and end dates for each activity
        activity_date_range = pd.date_range(start=row['Start_date'], end=row['End_date'], freq='D').tolist()
        df_container['Dates'] = activity_date_range
        df_container['SubTaskID'] = row['SubTaskID']
        df_container['SubTasks'] = row['SubTasks']
        df_container['Average_Accomplishment'] = row['Average_Accomplishement_per_Day']
        df_assembled = pd.concat([df_assembled, df_container], ignore_index=True)

    print("Data frame assembly completed.")
    print("Sample assembled data:")
    print(df_assembled.head())

    return df_assembled

def bussiness_calendar(holidays, weekdays_active): #tenant and project specific calendar

    print(section_separator)
    print("Setting up business calendar...")

    busDays = np.busdaycalendar(weekmask=weekdays_active, holidays=holidays)

    print("Business calendar setup completed.")
    return busDays

def business_days_integration(dataframe, busDays): #function to integrate business day to the dataframe

    print(section_separator)
    print("Integrating business days into the dataframe...")

    #business day array based on the 'Dates' column of df_assembled
    #creating a numpy array of datetime64[D] type from the 'Dates' column
    dates_np_array = dataframe['Dates'].values.astype('datetime64[D]')

    #creating the is business day array
    busDayArray = np.is_busday(dates_np_array, busdaycal=busDays)

    #integrating the business day array into the dataframe
    #create the new column 'Is a Business Day'
    dataframe['Is a Business Day'] = busDayArray

    #rearrqanging the columns
    dataframe = dataframe[[
        'Dates',
        'Is a Business Day',
        'SubTaskID',
        'SubTasks',
        'Average_Accomplishment'
    ]]

    print("Showing the first 20 rows of the integrated dataframe:")
    print(dataframe.head(20))
    print() # a blank line for better readability
    print("Business days integration completed.")
    print() # a blank line for better readability
    print("Finalizing the dataframe...")

    # creating a new column where instead of boolean business days values, it will be 1s and 0s
    dataframe = dataframe.copy()
    dataframe['Is a Business Day_int'] = dataframe['Is a Business Day'].astype(int)
    # Creating a new 'Average Accomplishment' columns where the accomplishment is only counted on business days(if business days is = 1)
    dataframe['Average_Accomplishment_Business_Days'] = dataframe['Average_Accomplishment'] * dataframe['Is a Business Day_int']
    #dropping columns 'Average_Accomplishment' and 'Is a Business Day_int' as they are no longer needed
    dataframe = dataframe.drop(columns=['Average_Accomplishment', 'Is a Business Day_int'])
    
    #perfoming  aggreagation on Dates column
    dataframe = dataframe.groupby('Dates')['Average_Accomplishment_Business_Days'].sum().reset_index()

    print("Dataframe finalization completed.")
    print("Showing the first 20 rows of the final dataframe:")
    print(dataframe.head(20))

    return dataframe

def chart_generator(dataframe, start, end): #function to generate charts.

    print(section_separator)
    print("Generating chart dataframe...")


    #generating date range for the x-axis
    date_range = pd.date_range(start=start, end=end, freq='D')
    df_daterange = pd.DataFrame({"Dates": date_range})
    #merging the date range with the final dataframe to ensure all dates are represented
    df_chart = pd.merge(df_daterange, dataframe, on='Dates', how='left')

    #fille missing values with 0
    df_chart["Average_Accomplishment_Business_Days"] = df_chart["Average_Accomplishment_Business_Days"].fillna(0)

    #crerating the cumulative accomplishment column
    df_chart["Cumulative_Accomplishment"] = df_chart["Average_Accomplishment_Business_Days"].cumsum()

    print("Chart dataframe generation completed.")
    print("Showing the first 20 rows of the chart dataframe:")
    print(df_chart.head(20))

    return df_chart

def output_generator(dataframe): #function to generate outout. This should be modified to write ti the actual database

    print(section_separator)
    print("Generating output...")


    #exporting df_assembled dataframe to a csv file
    dataframe.to_csv('Output/assembled_data.csv', index=False)

    #exporting df_assembled to a json file
    dataframe.to_json('Output/assembled_data.json', orient='records', date_format='iso')

    #exporting df_assembled to ndjson file
    dataframe.to_json('Output/assembled_data.ndjson', orient='records', date_format='iso', lines=True)

    print("Output generation completed.")
    print(section_separator)
    print(section_separator)



def __main__():

    # file_path can be modified to point to the actual database or data source
    file_path = "Data/planned.csv"

    # calling data ingestion function. Storing the output dataframe to a variable
    df_planned = data_ingestion(file_path=file_path)

    # calculating the business calendar. 
    busDays = bussiness_calendar(holidays=holidays, weekdays_active=weekdays_active)

    # calling activity weight calculation function. Storing the output dataframe to a variable
    df_processed = calculate_activity_weight(dataframe=df_planned, busDays=busDays)

    #calling dataframe assembly function and storing the output dataframe to a variable
    df_assembled = dataframe_assembly(dataframe=df_processed)

    #calling business days integration function
    df_final = business_days_integration(dataframe=df_assembled, busDays=busDays)

    #calling chart generator function
    df_chart = chart_generator(dataframe=df_final, start=start, end=end)

    #calling the output generator function
    output_generator(dataframe=df_chart)

if __name__ == "__main__":
    __main__()



