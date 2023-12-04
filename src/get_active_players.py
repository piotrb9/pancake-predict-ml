"""Get addresses of players that have been active in the last X days"""
import configparser
import requests

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
    if txs is None:fix
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

