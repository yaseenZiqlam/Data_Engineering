import json
import requests

GAME_IDS_API = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"

def fetch_all_game_ids():
    response = requests.get(GAME_IDS_API)
    response.raise_for_status()
    data = response.json()
    return [game['appid'] for game in data['applist']['apps']]

def main():
    game_ids = fetch_all_game_ids()
    with open("/home/sean/airflow/scripts/all_game_ids.json", "w") as f:
        json.dump(game_ids, f, indent=4)
    print(f"Successfully saved {len(game_ids)} game IDs.")

if __name__ == "__main__":
    main()

