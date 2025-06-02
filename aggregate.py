import os
import pandas as pd

def produce_aggregate_data(input_dir):
    """ Read all the csv files in the input directory that contains "street" in their name,
        aggregate the data by LSOA code and Crime type,
        put them all into a single dataframe
        and return the dataframe.
    """

    csv_files = os.listdir(input_dir)

    #filter for files that contain 'street' 
    street_files = [f for f in csv_files if 'street' in f]

    #create dataframe with the same columns as the first file
    #this will be used to append the dataframes 

    df = pd.read_csv(os.path.join(input_dir, street_files[0]))
    crime_counts = df.groupby('LSOA code')['Crime type'].value_counts().unstack(fill_value=0)
    crime_counts = crime_counts.reset_index()
    crime_counts.columns.name = None  # Remove the name of the index

    # Loop through the rest of the files and append the data
    for file in street_files[1:]:
        df = pd.read_csv(os.path.join(input_dir, file))
        crime_counts_temp = df.groupby('LSOA code')['Crime type'].value_counts().unstack(fill_value=0)
        crime_counts_temp = crime_counts_temp.reset_index()
        crime_counts_temp.columns.name = None  # Remove the name of the index
        crime_counts = pd.concat([crime_counts, crime_counts_temp], ignore_index=True)

    return crime_counts

def produce_aggregate_data_by_year(year, input_dir = r"C:\Users\THW_9\Desktop\LSE School\WT\GY460\S2 Project\Datasets\crime\2025_police_lsoa"):
    """ 
    Read all the montly folders in the input directory, 
    then read all the csv files in each folder that contains "street" in their name,
    aggregate the data by LSOA code and Crime type,
    put them all into a single dataframe
    and return the dataframe. 
    """

    monthly_folders = os.listdir(input_dir)

    #produce list of directories that contain the year
    year_directories = [f for f in monthly_folders if str(year) in f]

    #create empty dataframe to hold the dataframes
    main_crime_counts = pd.DataFrame()

    for directory in year_directories:

        dir = os.path.join(input_dir, directory)
        csvfiles = os.listdir( dir )

        #filter for files that contain 'street' 
        street_files = [f for f in csvfiles if 'street' in f]

        #create dataframe with the same columns as the first file
        #this will be used to append the dataframes 

        print(street_files[0])
        df = pd.read_csv(os.path.join(dir, street_files[0]))
        police_force = street_files[0].split('-street')[0]
        print(police_force)
        df['Police Force'] = police_force
        crime_counts = df.groupby(['LSOA code'])['Crime type'].value_counts().unstack(fill_value=0)
        crime_counts = crime_counts.reset_index()
        crime_counts.columns.name = None  # Remove the name of the index

        # Loop through the rest of the files and append the data
        for file in street_files[1:]:
            print(file, len(crime_counts))
            df = pd.read_csv(os.path.join(dir, file))
            police_force = file.split('-street')[0]
            print(police_force)
            df['Police Force'] = police_force
            crime_counts_temp = df.groupby(['LSOA code'])['Crime type'].value_counts().unstack(fill_value=0)
            crime_counts_temp = crime_counts_temp.reset_index()
            crime_counts_temp.columns.name = None  # Remove the name of the index
            crime_counts = pd.concat([crime_counts, crime_counts_temp], ignore_index=True)

        # Append the dataframe to the main dataframe
        main_crime_counts = pd.concat([main_crime_counts, crime_counts], ignore_index=True)
        print()
        print(directory, len(main_crime_counts))
        print()

    aggregated_df = main_crime_counts.groupby(['LSOA code'], as_index=False).sum(numeric_only=True)
    return aggregated_df

    def generateLSOA_police(input_dir = r"C:\Users\THW_9\Desktop\LSE School\WT\GY460\S2 Project\Datasets\crime\2025_police_lsoa\2022-04"):
        """
        Generate a df that tracks which police force is responsible for which LSOA codes
        """

        csv_files = os.listdir(input_dir)

        #generate a df with each of the csv file as a column
        lsoa_police_df = pd.DataFrame()

        street_files = [f for f in csvfiles if 'street' in f]

        for file in street_files:
            df = pd.read_csv(os.path.join(input_dir, file))
            police_force = file.split('-street')[0]
            df['Police Force'] = police_force
            lsoa_police_df = pd.concat([lsoa_police_df, df[['LSOA code', 'Police Force']]], ignore_index=True)

        return lsoa_police_df.drop_duplicates().reset_index(drop=True)
