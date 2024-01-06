#!/usr/bin/env python
# coding: utf-8

# In[23]:


#import dependant libraries

import json
import sqlite3


#create a flight data processor class and seal all methods 
class FlightDataProcessor:
    def __init__(self, json_file_path, db_file_path):
        
        #initiate the input and output file path
        self.json_file_path = json_file_path
        self.db_file_path = db_file_path

    def flatten_json(self, entry):
        
        flattened_data = {}

        def flatten(data, parent_key=''):
            if isinstance(data, dict):
                for k, v in data.items():
                    new_key = f"{parent_key}_{k}" if parent_key else k
                    flatten(v, new_key)
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    new_key = f"{parent_key}_{i}" if parent_key else str(i)
                    flatten(item, new_key)
            else:
                flattened_data[parent_key] = data

        flatten(entry)
        return flattened_data

    def load_data_from_json(self):
        with open(self.json_file_path) as json_file:
            data = json.load(json_file)
            # If the data is a list, assume it's a JSON array and extract each entry
            if isinstance(data, list):
                return data
            else:
                raise ValueError("The JSON file does not contain a valid array of entries.")

    # create the connection to sqlite3 database, then create a new table regarding flight information
    def create_database_table(self):
        conn = sqlite3.connect(self.db_file_path)
        cursor = conn.cursor()
        
        #saving year as integer data, country, iata_code and icao_code as text data, number of total passengers as integer
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flights (
                year INTEGER,
                country TEXT,
                iata_code TEXT,
                icao_code TEXT,
                total_passengers INTEGER
            )
        ''')
        conn.commit()
        conn.close()

    def save_data_to_database(self, data):
        conn = sqlite3.connect(self.db_file_path)
        cursor = conn.cursor()
        
        #extract year, codes and number of passengers from the data
        for entry in data:
            country = entry.get('country', '')
            airports = entry.get('airports', [])
            for airport in airports:
                year = entry.get('year', '')
                iata_code = airport.get('iata_code', '')
                icao_code = airport.get('icao_code', '')
                total_passengers = airport.get('total_passengers', '')

                # Check if total_passengers is not an empty string before converting to int
                if total_passengers and total_passengers.isdigit():
                    cursor.execute('''
                        INSERT INTO flights (year, country, iata_code, icao_code, total_passengers)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (year, country, iata_code, icao_code, int(total_passengers)))

        conn.commit()
        conn.close()

    def calculate_total_passengers_per_country(self):
        conn = sqlite3.connect(self.db_file_path)
        cursor = conn.cursor()
        
        #create query to calculate the sum of total passengers of each country
        cursor.execute('''
            SELECT country, SUM(total_passengers) as total_passengers
            FROM flights
            GROUP BY country
        ''')
        total_passengers_per_country = {country: total_passengers for country, total_passengers in cursor.fetchall()}
        conn.close()
        return total_passengers_per_country

    def process_data(self):
        
        #aggregate all processes together
        data = self.load_data_from_json()
        self.create_database_table()
        self.save_data_to_database(data)
        total_passengers_per_country = self.calculate_total_passengers_per_country()

        # Display the results
        print("Total Passengers per Country:")
        for country, total_passengers in total_passengers_per_country.items():
            print(f"{country}: {total_passengers}")


# Example usage
processor = FlightDataProcessor(json_file_path='data.json', db_file_path='flights.db')
processor.process_data()


# In[ ]:




