"""Check the players results (win ratio and profit) for a given period of time"""
import datetime
import pandas as pd
from simulator import simulate


def get_players_metrics(player_bet_df: pd.DataFrame, bet_amount_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the win ratio of each player
    :param player_bet_df: Dataframe with player bets
    :param bet_amount_df: Dataframe with player bet size
    :return: Dataframe with players metrics
    """

    players_metrics_list = []

    # Get the unique players
    not_players_list = ['epoch', 'start_timestamp', 'lock_timestamp', 'close_timestamp', 'lock_price', 'close_price',
                        'total_amount', 'bull_amount', 'bear_amount', 'position']

    assert player_bet_df.columns.to_list() == bet_amount_df.columns.to_list(), \
        "Columns of both dataframes must be the same"

    players_list = player_bet_df.columns.to_list()

    # Calculate metrics for each player
    for player in players_list:
        if player not in not_players_list:
            player_bets = player_bet_df[player]
            player_bet_sizes = bet_amount_df[player]

            simulation_df = simulate(player_bet_df[['epoch', 'position', 'bull_amount', 'bear_amount', 'total_amount']],
                                     player_bets, player_bet_sizes, add_bet_to_pool=False)

            simulation_df = simulation_df[simulation_df['bet_size'].notna()]

            win_bets = len(simulation_df[simulation_df['win'] == 1])
            total_bets = len(simulation_df)
            if total_bets == 0:
                print(f"Player {player} has no bets")
                continue
            win_ratio = win_bets / total_bets

            total_profit = simulation_df['profit'].sum()

            profit_per_bet = total_profit / total_bets

            # Add the player and win ratio to the dataframe
            players_metrics_list.append({'player': player, 'win_ratio': win_ratio, 'total_bets': total_bets,
                                         'total_profit': total_profit, 'profit_per_bet': profit_per_bet})

    return pd.DataFrame(players_metrics_list)


if __name__ == "__main__":
    time_from_training = int(datetime.datetime(2022, 10, 28, 0, 0).timestamp())
    time_to_training = int(datetime.datetime(2022, 12, 28, 0, 0).timestamp())

    print(f"{time_from_training}")

    from utils import load_players_data

    player_bet_df, bet_amount_df = load_players_data(time_from_training, time_to_training)

    players_win_ratio1 = get_players_metrics(player_bet_df, bet_amount_df)

    players_win_ratio1.sort_values('total_bets', ascending=False, inplace=True)
    print(players_win_ratio1)
