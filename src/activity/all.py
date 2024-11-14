"""
This code processes activity data for multiple participants by reading it from a PostgreSQL database
over an SSH connection. It classifies activity periods as either active or inactive based on a given
inactive threshold, then calculates the total active and inactive time for each participant within
specific daily observation windows (e.g., from 9 PM to 9 AM). The results are stored in individual
CSV files for each participant.
"""


from prometheus_client.samples import Timestamp
from sshtunnel import SSHTunnelForwarder
import psycopg2
import os
import pandas as pd
from datetime import timedelta


# Define observation windows
window_start_time = pd.Timestamp('21:00:00').time()  # Start time
window_end_time = pd.Timestamp('09:00:00').time()    # End time

# Define the inactive threshold
inactive_threshold = timedelta(minutes=30)

def read_activity_table_from_database():
    # SSH connection details.
    ssh_host = 'loki.research.cs.dal.ca'
    ssh_port = 22
    ssh_username = os.getenv('LOKI_USERNAME')
    ssh_password = os.getenv('LOKI_PASSWORD')

    # Database connection details.
    db_host = '127.0.0.1'
    db_port = 5432
    db_name = 'staging_db'
    db_user = os.getenv('STAGING_DB_USERNAME')
    db_password = os.getenv('STAGING_DB_PASSWORD')

    # Establish SSH tunnel and connect to PostgreSQL.
    try:
        with SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_username,
                ssh_password=ssh_password,
                remote_bind_address=('127.0.0.1', db_port)  # Forwarding PostgreSQL port.
        ) as tunnel:
            # Connect to PostgreSQL database through the SSH tunnel.
            conn = psycopg2.connect(
                host=db_host,
                port=tunnel.local_bind_port,  # use the local port set by the tunnel.
                dbname=db_name,
                user=db_user,
                password=db_password
            )

            print("Database connection established")

            activity_query = "SELECT * FROM study_prositvd.activity;"
            df_activity = pd.read_sql_query(activity_query, conn)

            # Close database connection.
            conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

    return df_activity

def get_total_duration(start, end, windows):
    """Calculates total duration from start to end over a list of windows

    :param start: start time
    :param end: end time
    :param windows: list of windows

    :return: total duration
    """

    total_duration = timedelta(seconds=0)
    for window in windows:
        window_start, window_end = window
        if window_end >= start and window_start <= end:
            overlap_start = max(window_start, start)
            overlap_end = min(window_end, end)
            total_duration += overlap_end - overlap_start
    return total_duration

def create_daily_window_list(first: pd.Timestamp, last: pd.Timestamp, window_start_time, window_end_time):

    # Create a list to store the windows
    windows = []

    # Initialize the current date to the day of 'first' at the start of the window
    current_date = first.normalize()  # sets time to 00:00:00 on 'first' date

    # Check if the end time belongs to the next day or the same day
    end_on_next_day = window_end_time < window_start_time

    # Loop through each day until reaching 'last'
    while current_date <= last:
        # Define the start of the window for the current day
        window_start = pd.Timestamp.combine(current_date, window_start_time)

        # Determine if the end of the window falls on the same or next day
        if end_on_next_day:
            window_end = pd.Timestamp.combine(current_date + timedelta(days=1), window_end_time)
        else:
            window_end = pd.Timestamp.combine(current_date, window_end_time)

        # Ensure the window fits within 'first' and 'last'
        if window_start < last and window_end > first:
            # Clip to bounds of 'first' and 'last'
            start = max(window_start, first)
            end = min(window_end, last)

            # Append the window to the list
            windows.append((start, end))

        # Move to the next day
        current_date += timedelta(days=1)

    return windows

def main():
    df_activity = read_activity_table_from_database()
    df_activity['is_stationary'] = df_activity['value0'].apply(lambda x: int(x.split(', ')[1]))
    df_activity['is_walking'] = df_activity['value0'].apply(lambda x: int(x.split(', ')[3]))
    df_activity['is_running'] = df_activity['value0'].apply(lambda x: int(x.split(', ')[5]))
    df_activity['is_in_vehicle'] = df_activity['value0'].apply(lambda x: int(x.split(', ')[7]))
    df_activity['is_cycling'] = df_activity['value0'].apply(lambda x: int(x.split(', ')[9]))
    df_activity['is_activity_unknown'] = df_activity['value0'].apply(lambda x: int(x.split(', ')[11]))
    df_activity['confidence'] = df_activity['value0'].apply(lambda x: 1 if x.split(', ')[13] == 'High' else 0)
    df_activity.drop('_id', axis=1, inplace=True)
    df_activity.drop('value0', axis=1, inplace=True)
    df_activity.drop('uploadedat', axis=1, inplace=True)

    # Filter out rows with low confidence and unknown activities.
    df_known_confident = df_activity[(df_activity.is_activity_unknown == 0) & (df_activity.confidence == 1)]

    participant_ids = df_known_confident['participantid'].drop_duplicates().tolist()
    for participant_id in participant_ids:
        df = df_known_confident[df_known_confident.participantid == participant_id]
        df['is_active'] = (
            (df['is_stationary'].apply(lambda x: x == 1) & df['is_in_vehicle'].apply(lambda x: x == 0)).apply(
                lambda x: 0 if x else 1))

        df.drop('is_stationary', axis=1, inplace=True)
        df.drop('is_walking', axis=1, inplace=True)
        df.drop('is_running', axis=1, inplace=True)
        df.drop('is_in_vehicle', axis=1, inplace=True)
        df.drop('is_cycling', axis=1, inplace=True)
        df.drop('is_activity_unknown', axis=1, inplace=True)
        df.drop('confidence', axis=1, inplace=True)
        df.drop('participantid', axis=1, inplace=True)

        # Extract sorted timestamps where `is_active` is 1
        active_timestamps = df[df['is_active'] == 1].sort_values(by='measuredat')['measuredat'].tolist()

        # Initialize empty list to store active windows
        active_windows = []

        # Initialize the first window
        start_time = active_timestamps[0]
        end_time = active_timestamps[0]

        # Iterate through the timestamps
        for i in range(1, len(active_timestamps)):
            current_time = active_timestamps[i]
            previous_time = active_timestamps[i - 1]

            # If the difference is greater than the inactive threshold, close the current window
            if current_time - previous_time > inactive_threshold:
                active_windows.append((start_time, end_time))
                start_time = current_time  # Start a new window

            end_time = current_time  # Update end time for the current window

        # Append the last window
        active_windows.append((start_time, end_time))

        # Create the list of inactive windows
        inactive_windows = []

        # Start with the first inactive window, which starts at the end of the first active window
        prev_end = active_windows[0][1]

        for window in active_windows[1:]:
            current_start = window[0]

            # If there’s a gap between two active windows, it becomes an inactive window
            if current_start - prev_end > timedelta(seconds=0):
                inactive_windows.append((prev_end, current_start))

            # Update previous window end
            prev_end = window[1]

        # First and the last valid timestamps for this participant.
        if len(active_windows) > 0 and len(inactive_windows) > 0:
            first = min(active_windows[0][0], inactive_windows[0][0])
            last = max(active_windows[-1][-1], inactive_windows[-1][-1])
        elif len(active_windows) > 0:
            first = active_windows[0][0]
            last = active_windows[-1][-1]
        elif len(inactive_windows) > 0:
            first = inactive_windows[0][0]
            last = inactive_windows[-1][-1]
        else:
            # Both active_windows and inactive_windows are empty, so continue
            continue

        daily_windows = create_daily_window_list(first, last, window_start_time, window_end_time)
        rows = []
        for daily_window in daily_windows:
            daily_active_time = get_total_duration(daily_window[0], daily_window[1], active_windows)
            daily_inactive_time = get_total_duration(daily_window[0], daily_window[1], inactive_windows)

            row = (daily_window, daily_active_time, daily_inactive_time)
            rows.append(row)

        # Convert the list of rows into a DataFrame
        df = pd.DataFrame(rows, columns=["Daily Window", "Daily Active Time", "Daily Inactive Time"])

        # Save the DataFrame to a CSV file
        os.makedirs("results", exist_ok=True)
        df.to_csv(f"results/{participant_id}.csv", index=False)

if __name__ == '__main__':
    main()
