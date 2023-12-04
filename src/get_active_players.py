"""Get addresses of players that have been active in the last X days"""
import configparser
import datetime
import requests
import pandas as pd

api_config = configparser.ConfigParser()
api_config.read('../data/api_keys.ini')

bscscan_api_key = api_config.get('api', 'bscscan_api_key')


def get_txs(endblock: int) -> list:
    """
    Get all the transactions from the Pancake Prediction V3 contract address
    :param endblock: block number to get txs to
    :return: List of dicts with txs
    """
    url = f"https://api.bscscan.com/api?module=account&action=txlist&address=" \
          f"0x0E3A8078EDD2021dadcdE733C6b4a86E51EE8f07&startblock=0&endblock={endblock}&page=1&offset=10000&sort=" \
          f"desc&apikey={bscscan_api_key}"

    response = requests.request("GET", url)

    txs = response.json()
    txs = txs['result']

    txs_list = []
    if txs is None:
        return []

    for tx in txs:
        if "betBear" in tx['functionName'] or "betBull" in tx['functionName']:
            if int(tx['isError']) == 1:
                continue
            player = tx['from']
            gas_price = tx['gasPrice']
            block = tx['blockNumber']
            timestamp = int(tx['timeStamp'])

            tx_dict = {'player': player, 'gas_price': gas_price, 'block': block, 'timestamp': timestamp}

            txs_list.append(tx_dict)

    return txs_list


def get_block_number_by_timestamp(timestamp: int) -> int:
    """
    Get BSC block number
    :param timestamp: Timestamp to get block number for
    :return: BSC block number
    """
    url = f"https://api.bscscan.com/api?module=block&action=getblocknobytime&timestamp={timestamp}&closest=" \
          f"before&apikey={bscscan_api_key}"

    response = requests.request("GET", url)

    block = response.json()
    block = block['result']

    return int(block)


def save_active_players_to_csv(df: pd.DataFrame, active_players_file: str) -> None:
    """
    Save dataframe to csv file
    :param df: Dataframe to save
    :param active_players_file: Directory to save the file to
    :return: None
    """
    df.to_csv(active_players_file, header=True, index=False)


def make_active_players_file(timestamp_from: int, timestamp_to: int, number_of_days_to_check: int,
                             active_players_file: str) -> None:
    """
    Make active players CSV file
    :param timestamp_from: Timestamp to select the txs from
    :param timestamp_to: Timestamp to select the txs to
    :param number_of_days_to_check: Number of days to check - starting from current day block number backwards
    :param active_players_file: Directory to save the file to
    :return: None
    """
    txs = []

    block_number = get_block_number_by_timestamp(timestamp_to)

    for i in range(number_of_days_to_check):
        txs_part = get_txs(block_number - i * 28233)  # subtract number of daily blocks
        txs.extend(txs_part)

    df = pd.DataFrame(txs)

    df.sort_values('timestamp')
    print(f"Current range of downloaded data: {datetime.datetime.fromtimestamp(df['timestamp'].min())}"
          f" - {datetime.datetime.fromtimestamp(df['timestamp'].max())}")
    print(f"Selecting records within {datetime.datetime.fromtimestamp(timestamp_from)}"
          f" - {datetime.datetime.fromtimestamp(timestamp_to)}")

    df = df[df['timestamp'] >= float(timestamp_from)]
    df = df[df['timestamp'] <= float(timestamp_to)]

    before_dropping_duplicates_size = df.shape[0]
    df = df.drop_duplicates(keep='first')
    after_dropping_duplicates_size = df.shape[0]

    print(f"Removed {before_dropping_duplicates_size - after_dropping_duplicates_size} duplicates")

    counts_df = df['player'].value_counts().reset_index()
    counts_df.columns = ['player', 'count']

    save_active_players_to_csv(counts_df, active_players_file)
