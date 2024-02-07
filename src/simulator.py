"""Simulate betting"""
import datetime

import pandas as pd
import numpy as np
from utils import load_players_data


def copy_trade_player(player_bet_df: pd.DataFrame, bet_amount_df: pd.DataFrame, player_address: str) -> pd.DataFrame:
    """
    Copy all trades of a player in a given period of time
    :param player_bet_df: Dataframe with player bets
    :param bet_amount_df: Dataframe with player bet size
    :param player_address: Address of the player to copy the trades of
    :return: Dataframe with copied trades
    """

    player_bets = player_bet_df[player_address]
    player_bet_sizes = bet_amount_df[player_address]

    trading_data = simulate(player_bet_df[['epoch', 'position', 'bull_amount', 'bear_amount', 'total_amount']],
                            player_bets, player_bet_sizes, add_bet_to_pool=True)

    return trading_data


def simulate(data_df: pd.DataFrame, prediction: pd.Series, bet_size: pd.Series,
             add_bet_to_pool: bool = True) -> pd.DataFrame:
    """
    Simulate the bet game based on the prediction and bet_size
    :param data_df: Dataframe with the needed data, can be bet_amount_df or player_bet_df. Need only cols: epoch,
    position, bull_amount, bear_amount, total_amount
    :param prediction: Series with predictions for each epoch
    :param bet_size: Series with bet sizes (in CAKE tokens) for each epoch. 0 means no bet
    :param add_bet_to_pool: If True, add the bet to the pool, otherwise don't (When analyzing the players results, we
    don't want to add the bets to the pool, so set it to False)
    :return: Dataframe with the simulation results (in CAKE tokens)
    """

    data_df = data_df.copy()  # To avoid SettingWithCopyWarning
    data_df['prediction'] = prediction
    data_df['bet_size'] = bet_size

    if not add_bet_to_pool:
        add_to_pool = 0
    else:
        add_to_pool = data_df['bet_size']

    # Change the labels: Bull -> 1 and Bear -> 0
    data_df['prediction'] = np.where(data_df['prediction'] == 'Bull', 1, data_df['prediction'])
    data_df['prediction'] = np.where(data_df['prediction'] == 'Bear', 0, data_df['prediction'])

    data_df['position'] = np.where(data_df['position'] == 'Bull', 1, data_df['position'])
    data_df['position'] = np.where(data_df['position'] == 'Bear', 0, data_df['position'])

    # Add CAKE tokens to the pool
    data_df['bull_amount'] = np.where(data_df['prediction'] == 1,
                                      data_df['bull_amount'] + add_to_pool,
                                      data_df['bull_amount'])

    data_df['bear_amount'] = np.where(data_df['prediction'] == 0,
                                      data_df['bear_amount'] + add_to_pool,
                                      data_df['bear_amount'])

    data_df['total_amount'] = data_df['bull_amount'] + data_df['bear_amount']

    # Calculate the multipliers, if the player bet on the winning position
    data_df['bull_multiplier'] = np.where(data_df['bull_amount'] > 0,
                                          data_df['total_amount'] / data_df['bull_amount'],
                                          1)

    data_df['bear_multiplier'] = np.where(data_df['bear_amount'] > 0,
                                          data_df['total_amount'] / data_df['bear_amount'],
                                          1)

    data_df['win'] = np.where(data_df['position'] == data_df['prediction'], 1, 0)

    data_df['multiplier'] = np.where(data_df['position'] == 1,
                                     data_df['bull_multiplier'],
                                     data_df['bear_multiplier'])

    data_df['multiplier'] = np.where(data_df['position'] == 'House', 0, data_df['multiplier'])

    data_df['profit'] = data_df['win'] * data_df['multiplier'] * data_df['bet_size'] - data_df['bet_size']

    # Pancake prediction v3 contract takes 3% of the profit
    data_df['profit'] = np.where(data_df['profit'] > 0, data_df['profit'] * 0.97, data_df['profit'])

    return data_df


if __name__ == "__main__":
    time_from_training = int(datetime.datetime(2022, 10, 28, 0, 0).timestamp())
    time_to_training = int(datetime.datetime(2022, 12, 28, 0, 0).timestamp())

    print(f"{time_from_training}")

    player_bet_df, bet_amount_df = load_players_data(time_from_training, time_to_training)

    # Get all wallets from player_bet_df
    wallets = player_bet_df.columns.to_list()
    not_players_list = ['epoch', 'start_timestamp', 'lock_timestamp', 'close_timestamp', 'lock_price', 'close_price',
                        'total_amount', 'bull_amount', 'bear_amount', 'position']
    wallets = [wallet for wallet in wallets if wallet not in not_players_list]

    for wallet in wallets:
        wallet_bets = player_bet_df[wallet]
        wallet_bet_sizes = bet_amount_df[wallet]

        simulate(player_bet_df[['epoch', 'position', 'bull_amount', 'bear_amount', 'total_amount']], wallet_bets, wallet_bet_sizes)
