import pandas as pd

def extract_csv():
    # Read the file line by line
    with open("C:/Users/ok/Downloads/data.csv", 'r') as file:
        lines = file.readlines()


    lines_as_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
    cols = lines_as_lists.pop(0)

    return lines_as_lists, cols

def transfomr_df():

    lines_as_lists, cols = extract_csv()

    vehicle_col = cols[:4]
    trejictory_col = ['track_id'] + cols[4:]

    track_info = []
    trajectory_info = []

    for row in lines_as_lists:
        track_id = row[0]

        # add the first 4 values to track_info
        track_info.append(row[:4])

        remaining_values = row[4:]
        # reshape the list into a matrix and add track_id
        trajectory_matrix = [ [track_id] + remaining_values[i:i+6] for i in range(0,len(remaining_values),6)]
        # add the matrix rows to trajectory_info
        trajectory_info = trajectory_info + trajectory_matrix

    #now lets convert them into dataframes
    vehicle_data = pd.DataFrame(track_info, columns=vehicle_col)
    trajectory_data = pd.DataFrame(trajectory_info, columns=trejictory_col)

    return vehicle_data, trajectory_data

