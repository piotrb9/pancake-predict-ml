"""
Utility functions for the Pancake Prediction v3 data analysis
"""
import datetime
import pandas as pd


def load_players_data(timestamp_from: int, timestamp_to: int,
                      data_dir: str = '../data/merged_data/') -> (pd.DataFrame, pd.DataFrame):
    """
    Load the data of the players from 2 csv files: final_player_bet.csv and final_bet_amount.csv
    :param timestamp_from: Timestamp to select the bets from
    :param timestamp_to: Timestamp to select the bets to
    :param data_dir: Directory where the data is stored, default is '../data/merged_data/'
    :return: Dataframes with player bets and bet sizes
    """
    player_bet_df = pd.read_csv(f'{data_dir}final_player_bet.csv', low_memory=False)
    bet_amount_df = pd.read_csv(f'{data_dir}final_bet_amount.csv', low_memory=False)

    # Filter the dataframes by timestamp
    player_bet_df = player_bet_df[(player_bet_df['start_timestamp'] >= timestamp_from) &
                                  (player_bet_df['start_timestamp'] <= timestamp_to)]

    bet_amount_df = bet_amount_df[(bet_amount_df['start_timestamp'] >= timestamp_from) &
                                  (bet_amount_df['start_timestamp'] <= timestamp_to)]

    # Change the data types of the columns
    int_cols = ['epoch', 'start_timestamp', 'lock_timestamp', 'close_timestamp', 'lock_price', 'close_price']
    player_bet_df[int_cols] = player_bet_df[int_cols].astype(int)

    float_cols = ['total_amount', 'bull_amount', 'bear_amount']
    player_bet_df[float_cols] = player_bet_df[float_cols].astype(float) / 10 ** 18

    category_cols = [col for col in player_bet_df.columns if col not in int_cols + float_cols]
    common_categories = ['Bull', 'Bear', 'House']  # Categories have to be set manually, as not all columns contain 'House'

    player_bet_df[category_cols] = player_bet_df[category_cols].astype('category')
    for col in category_cols:
        player_bet_df[col] = player_bet_df[col].cat.set_categories(common_categories)

    # Same for the second dataframe
    bet_amount_df[int_cols] = bet_amount_df[int_cols].astype(int)

    bet_amount_df['position'] = bet_amount_df['position'].astype('category')
    bet_amount_df['position'] = bet_amount_df['position'].cat.set_categories(common_categories)

    float_cols = [col for col in bet_amount_df.columns if col not in int_cols + ['position']]
    bet_amount_df[float_cols] = bet_amount_df[float_cols].astype(float) / 10 ** 18

    return player_bet_df, bet_amount_df


if __name__ == "__main__":
    time_from_training = int(datetime.datetime(2022, 10, 28, 0, 0).timestamp())
    time_to_training = int(datetime.datetime(2022, 12, 28, 0, 0).timestamp())

    print(f"{time_from_training}")

    load_players_data(time_from_training, time_to_training)
