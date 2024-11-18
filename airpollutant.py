import requests
import argparse
import time
from config import API_TOKEN
from concurrent.futures import ThreadPoolExecutor, as_completed

"""" This script takes in two pairs of longitude and latitude values 
 and a sampling period and sampling rate value from the user 
 It will compute the PM2.5 value for a location over the sampling period 
 and the average for all stations in the sampling period """

# Given pairs of coordinates, request and return stations within the area from an API
def get_station_ids(latitude_1, longitude_1, latitude_2, longitude_2):
    # Define the coordinates of interest
    coordinates = "{},{},{},{}".format(latitude_1, longitude_1, latitude_2, longitude_2)
    url = f'https://api.waqi.info/map/bounds?token={API_TOKEN}&latlng={coordinates}'
    try:
        response = requests.get(url)  # Fetch air quality station ID's within the coordinates from API
        response.raise_for_status()  # Raise an error if the HTTP request was unsuccessful
        data = response.json()  # Get the response data
        if 'data' in data:  # Return list of stations of successful
            return [station['uid'] for station in data['data']], None
        else:
            return None, "No station data found"
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException) as e:  # Catch any errors
        return None, e


# Given a station ID, query the API for PM2.5 data and return it if successful
def get_air_data(station_id):
    url = f'https://api.waqi.info/feed/@{station_id}/?token={API_TOKEN}'
    try:
        response = requests.get(url)  # Fetch data for given station ID FROM API
        response.raise_for_status()  # Raise an error if the HTTP request was unsuccessful
        air_data = response.json()  # Get the response data
        return air_data, None  # Return data if found
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException) as e:  # Catch any errors
        return None, e


# Given a list of station IDs, concurrently fetch PM2.5 data for each station and store the name and value
def get_data_for_stations(station_ids):
    data_list = []
    with ThreadPoolExecutor() as executor:  # Create a thread pool default workers to be number of CPUs * 5
        futures = {executor.submit(get_air_data, station_id): station_id for station_id in station_ids} # Submit a task to the thread pool for each station id
        for future in as_completed(futures):  # For each future object
            station_id = futures[future]  # Get the corresponding station ID
            try:
                air_data, error = future.result()  # Extract the data for the future
                # If data is found then extract the station name and value and append it to the data list
                if air_data and 'data' in air_data and 'iaqi' in air_data['data'] and 'pm25' in air_data['data']['iaqi']:
                    station_name = air_data['data']['city']['name']
                    value = air_data['data']['iaqi']['pm25']['v']
                    data_list.append({'station_name': station_name, 'pm25': value})
                else:  # Else print any errors that were caught
                    print(f"Error for station ID {station_id}: {error}")
            except Exception as exc:
                print(f"Error fetching data for station ID {station_id}: {exc}")
    return data_list


# Given pairs of coordinates, check if they are within a valid range
def validate_coordinates(latitude_1, longitude_1, latitude_2, longitude_2):
    # Latitude has to be in a range of -90 and 90 and longitude has to be in a range of -180 and 180
    if not (-90 <= latitude_1 <= 90):
        raise ValueError("Latitude 1 must be between -90 and 90.")
    if not (-180 <= longitude_1 <= 180):
        raise ValueError("Longitude 1 must be between -180 and 180.")
    if not (-90 <= latitude_2 <= 90):
        raise ValueError("Latitude 2 must be between -90 and 90.")
    if not (-180 <= longitude_2 <= 180):
        raise ValueError("Longitude 2 must be between -180 and 180.")


# Print the PM2.5 sampled values for each station in the list
def print_sampled_value(data_list):
    print("Sampled values:")
    for item in data_list:
        print(f"{item['station_name']}: {item['pm25_values']}")


# Print the overall PM2.5 average for each station
def print_overall_average_value(data_list):
    print("Overall average of all samples:")
    for item in data_list:
        if item['pm25_values'] is not None:
            # Compute the average and print it
            average = sum(item['pm25_values']) / len(item['pm25_values'])
            print(f"{item['station_name']}: {average:.2f}")
        else:
            print(f"{item['station_name']}: No data available")


def main(latitude_1, longitude_1, latitude_2, longitude_2, sampling_period, sampling_rate):
    # Get the station IDs for the given coordinates
    station_ids, error = get_station_ids(latitude_1, longitude_1, latitude_2, longitude_2)
    if error:
        print("Error requesting station IDs:", error)
        return

    # Compute total samples
    total_samples = sampling_period * sampling_rate
    data_list = []
    # Get the data for each station
    for i in range(total_samples):
        station_data = get_data_for_stations(station_ids)
        # For each station in the list
        for data in station_data:
            # Get the station's name and pm25 value
            station_name = data['station_name']
            value = data['pm25']
            station_entry = None
            # For each item in the data list
            for item in data_list:
                # Check if station already exists in the data list
                if item['station_name'] == station_name:
                    station_entry = item
                    break
            # If station already exists that we add the pm25 value to the station
            if station_entry:
                station_entry['pm25_values'].append(value)
            # Else we create new entry for the station if not already in list
            else:
                data_list.append({'station_name': station_name, 'pm25_values': [value]})
        if i < total_samples - 1: # If we are not at the last sample yet
            time.sleep(60 / sampling_rate) # Sleep so that we get the sampling rate

    # Print the PM2.5 data for each station
    print_sampled_value(data_list)
    print_overall_average_value(data_list)


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Calculate PM2.5 average for given map bounds")
    parser.add_argument("latitude_1", type=float, help="First latitude for map bounds")
    parser.add_argument("longitude_1", type=float, help="First longitude for map bounds")
    parser.add_argument("latitude_2", type=float, help="Second latitude for map bounds")
    parser.add_argument("longitude_2", type=float, help="Second longitude for map bounds")
    parser.add_argument("sampling_period", type=int, default=5, help="Sampling period in minutes (default=5)")
    parser.add_argument("sampling_rate", type=int, default=1, help="Sampling rate per minute (default=1)")

    # Parse arguments
    args = parser.parse_args()

    try:
        # Validate coordinates
        validate_coordinates(args.latitude_1, args.longitude_1, args.latitude_2, args.longitude_2)
        # Call main function with parsed arguments
        main(args.latitude_1, args.longitude_1, args.latitude_2, args.longitude_2, args.sampling_period, args.sampling_rate)
    except ValueError as ve:
        print("Input error:", ve)
    except Exception as e:
        print("An unexpected error occurred:", e)
