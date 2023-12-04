"""Download players bets hisory using bscscan api"""
import configparser
import json
import os
import requests
from web3_input_decoder import decode_function
import concurrent.futures

api_config = configparser.ConfigParser()
api_config.read('../data/api_keys.ini')

bscscan_api_key = api_config.get('api', 'bscscan_api_key')


def get_bets(address: str, start_block: int = 0, end_block: int = 99999999) -> list:
    """
    Get all the Pancake Prediction v3 bets for a given address
    :param address: Wallet address
    :param start_block: Block number to start searching from (use 0 to search from the beginning), optional
    :param end_block: Block number to end searching at (use 99999999 to search until the end), optional
    :return: List of dicts with bets
    """
    pancake_prediction_v3_abi = json.load(open("../data/pancake_prediction_v3_abi.json", 'r'))

    url = f"https://api.bscscan.com/api?module=account&action=txlist&address={address}&startblock={start_block}&" \
          f"endblock={end_block}&page=1&offset=10000&sort=desc&apikey={bscscan_api_key}"

    response = requests.request("GET", url)

    bets = response.json()
    all_bets = bets['result']

    if len(all_bets) == 10000:
        print("There are more than 10000 transactions, downloading more transactions")
        all_bets = []

        # Divide the block range into 20 parts
        block_range = end_block - start_block
        block_range_part = block_range // 20

        # Loop to download the transactions, since the api only allows to download 10000 transactions at a time
        for i in range(0, 20):
            url = f"https://api.bscscan.com/api?module=account&action=txlist&address={address}&startblock=" \
                  f"{start_block+i*block_range_part}&endblock={start_block+(i+1)*block_range_part}&page=1&offset=" \
                  f"10000&sort=desc&apikey={bscscan_api_key}"

            response = requests.request("GET", url)

            bets = response.json()
            bets = bets['result']

            all_bets.extend(bets)

    good_bets = []
    already_checked = []
    if all_bets is None:
        return []
    for bet in all_bets:
        if bet['functionName'] == "betBear(uint256 _index, uint256 amount)" \
                or bet['functionName'] == "betBull(uint256 _index, uint256 amount)":

            bet_input = decode_input(bet, pancake_prediction_v3_abi)

            if bet['functionName'] == "betBear(uint256 _index, uint256 amount)":
                bet['functionName'] = "Bear"

            elif bet['functionName'] == "betBull(uint256 _index, uint256 amount)":
                bet['functionName'] = "Bull"

            bet['input'] = bet_input
            bet['bet_amount'] = bet_input[1][2]
            bet['epoch'] = bet_input[0][2]

            if bet['epoch'] not in already_checked:
                already_checked.append(bet['epoch'])
                good_bets.append(bet)

    return good_bets


def decode_input(transaction: dict, abi: list) -> list:
    """
    Decode the input of a transaction using the abi
    :param transaction: Transaction dict
    :param abi: Pancake prediction v3 abi
    :return: Decoded transaction input
    """
    decoded_input = decode_function(abi, transaction['input'])

    return decoded_input


def main_concurrent(addresses: list, players_data_folder: str, start_block: int = 0, end_block: int = 99999999) -> None:
    """
    Download data for multiple addresses concurrently
    :param addresses: List of addresses
    :param players_data_folder: Directory to save the data
    :param start_block: Block number to start searching from (use 0 to search from the beginning), optional
    :param end_block: Block number to end searching at (use 99999999 to search until the end), optional
    :return: None
    """
    def simple_address_function(address: str) -> None:
        """
        Simple function to download data for a single address and save it to a .json file
        :param address: Address to download data for
        :return: None
        """
        print(f"Downloading data of {address}")
        bets = get_bets(address, start_block, end_block)

        with open(players_data_folder + address + '.json', 'w') as file:
            json.dump(bets, file)

        print(f"Done with {address}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        # Check the players_data_folder for .json files already existing and remove them from the addresses list
        current_files = os.listdir(players_data_folder)
        current_files = [file.replace(".json", "") for file in current_files]
        addresses = [address for address in addresses if address not in current_files]

        print(f"Downloading data for {len(addresses)} addresses")

        for address in addresses:
            futures.append(executor.submit(simple_address_function, address=address))

