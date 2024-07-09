#!/bin/bash

# This script performs data preprocessing tasks

# Remove unwanted characters from the JSON data file
sed -i 's/\\n//g' /home/sean/airflow/scripts/game_details.json

# Extract selected features from JSON and save to a new JSON file
python3 <<EOF
import json

selected_features = ['name', 'steam_appid', 'required_age', 'is_free', 'price_overview', 'controller_support', 
                     'about_the_game', 'website', 'supported_languages',
                     'genres', 'ratings', 'reviews']

# Load the raw data
try:
    with open('/home/sean/airflow/scripts/game_details.json', 'r') as f:
        data = json.load(f)
except Exception as e:
    print(f"Error loading JSON data: {e}")
    exit(1)

# Extract the selected features
processed_data = []
for game in data:
    if game:  # Ensure the game data is not empty
        processed_game = {feature: game.get(feature, None) for feature in selected_features}
        processed_data.append(processed_game)

# Save the processed data
try:
    with open('/home/sean/airflow/scripts/processed_game_details.json', 'w') as f:
        json.dump(processed_data, f, indent=4)
except Exception as e:
    print(f"Error saving JSON data: {e}")
    exit(1)

print("Preprocessing complete")
EOF


