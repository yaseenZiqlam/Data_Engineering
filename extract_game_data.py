import json
import requests
from time import sleep
from tenacity import retry, stop_after_attempt, wait_fixed
import logging

logging.basicConfig(level=logging.INFO, filename='/home/sean/airflow/logs/data_extraction.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s')

DETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={}"

@retry(stop=stop_after_attempt(5), wait=wait_fixed(60))
def fetch_game_data(game_id):
    response = requests.get(DETAILS_URL.format(game_id))
    response.raise_for_status()
    data = response.json()
    return data[str(game_id)]['data'] if data[str(game_id)]['success'] else None

def main(start_index):
    with open("/home/sean/airflow/scripts/all_game_ids.json", "r") as f:
        game_ids = json.load(f)

    end_index = start_index + 1000
    current_chunk = game_ids[start_index:end_index]
    game_details = []
    processed_count = 0

    for game_id in current_chunk:
        try:
            logging.info(f"Processing game ID: {game_id}")
            game_data = fetch_game_data(game_id)
            if game_data:
                game_details.append({
                    "steam_appid": game_data.get("steam_appid"),
                    "name": game_data.get("name"),
                    "required_age": game_data.get("required_age"),
                    "is_free": game_data.get("is_free"),
                    "price_overview": game_data.get("price_overview"),
                    "controller_support": game_data.get("controller_support"),
                    "about_the_game": game_data.get("about_the_game"),
                    "website": game_data.get("website"),
                    "supported_languages": game_data.get("supported_languages"),
                    "genres": ', '.join([genre['description'] for genre in game_data.get("genres", [])]),
                    "reviews": game_data.get("reviews"),
                })
                processed_count += 1
            else:
                logging.warning(f"No valid data for game ID {game_id}.")
        except Exception as e:
            logging.error(f"Error processing game ID {game_id}: {e}")

    if game_details:
        with open("/home/sean/airflow/scripts/game_details.json", "w") as f:
            json.dump(game_details, f, indent=4)
        logging.info(f"Successfully saved {len(game_details)} game details.")

    with open("/home/sean/airflow/scripts/processed_count.txt", "w") as f:
        f.write(str(processed_count))
    logging.info(f"Processed count for this run: {processed_count}")

if __name__ == "__main__":
    import sys
    start_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    main(start_index)
