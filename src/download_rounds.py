"""More advanced script to download rounds using web3 py"""
import configparser
import json
from web3 import Web3
import pandas as pd

pan_predictionv3_address = "0x0E3A8078EDD2021dadcdE733C6b4a86E51EE8f07"

api_config = configparser.ConfigParser()
api_config.read('../data/api_keys.ini')

quicknode_endpoint = api_config.get('api', 'quicknode_endpoint')


class RoundsDownloader:
    def __init__(self):
        self.web3 = Web3(Web3.HTTPProvider(quicknode_endpoint))
        self.chain_id = self.web3.eth.chain_id

        if self.web3.is_connected():
            print("Connection Successful")
        else:
            raise Exception("Connection failed")

        pancake_prediction_v3_abi = json.load(open("../data/pancake_prediction_v3_abi.json", 'r'))

        self.contract = self.web3.eth.contract(address=self.web3.to_checksum_address(pan_predictionv3_address),
                                               abi=pancake_prediction_v3_abi)

    def get_round_info(self, epoch: int) -> dict:
        """
        Get the info of a round
        :param epoch: Epoch number
        :return: Dict with round info
        """
        info = self.contract.functions.rounds(epoch).call()

        position = None
        if info[4] < info[5]:
            position = 'Bull'
        elif info[4] > info[5]:
            position = 'Bear'
        elif info[4] == info[5]:
            position = 'House'

        info_dict = {'epoch': info[0],
                     'start_timestamp': info[1],
                     'lock_timestamp': info[2],
                     'close_timestamp': info[3],
                     'lock_price': info[4],
                     'close_price': info[5],
                     'total_amount': info[8],
                     'bull_amount': info[9],
                     'bear_amount': info[10],
                     'position': position
                     }

        return info_dict

    def download_rounds(self, start_epoch: int, stop_epoch: int) -> list:
        """
        Download rounds data from start_epoch to stop_epoch
        :param start_epoch: Start epoch
        :param stop_epoch: Stop epoch
        :return: List of dicts with rounds data
        """
        rounds = []
        for i in range(start_epoch, stop_epoch):
            print(f"Downloading round {i}")
            info = self.get_round_info(i)

            rounds.append(info)

        return rounds

    def save_rounds(self, path: str, data: list) -> None:
        """
        Save the rounds data to a csv file
        :param path: Path to save the file
        :param data: List of dicts with rounds data
        :return: None
        """
        final_df = pd.DataFrame(data)

        print("------------------- FINAL DF --------------------")
        print(final_df)

        final_df = final_df.sort_values('epoch')
        final_df = final_df.reset_index(drop=True)

        final_df.to_csv(path, sep='\t', encoding='utf-8')


if __name__ == "__main__":
    downloader = RoundsDownloader()

    # Download rounds settings
    from_round = 36000
    to_round = 65000
    # download_rounds_file = 'final_rounds_data.csv'
    path = '../data/rounds_data/final_rounds_data.csv'

    data = downloader.download_rounds(from_round, to_round)  # Download rounds FROM-TO, we only need the last few months
    downloader.save_rounds(path, data)
