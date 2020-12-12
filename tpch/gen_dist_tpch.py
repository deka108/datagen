#!/usr/bin/env python

"""Generate distributed data out of tpc h data

Usage: ./gen_dist_tpch.py --help
"""
import argparse
import math
from datetime import datetime

import numpy as np
import os
import pandas as pd
import sqlite3


def generate_equal_split(df, n):
    size = len(df) // n
    sections = [i * (size + 1) for i in range(1, n)]
    return np.array_split(df, sections)


def generate_left_skew_split(df, n, skew=math.e):
    arr = [i ** skew for i in range(1, n + 1)]
    prob = [i / sum(arr) for i in arr]
    arr = [math.ceil(len(df) * p) for p in prob][::-1]
    sections = np.cumsum(arr)[:-1]
    return np.array_split(df, sections)


def generate_right_skew_split(df, n, skew=math.e):
    arr = [i ** skew for i in range(1, n + 1)]
    prob = [i / sum(arr) for i in arr]
    arr = [math.ceil(len(df) * p) for p in prob]
    sections = np.cumsum(arr)[:-1]
    return np.array_split(df, sections)


def generate_random_split(df, n):
    arr = np.random.randint(1, len(df), n + 1)
    prob = [i / sum(arr) for i in arr]
    arr = [math.ceil(len(df) * p) for p in prob]
    sections = np.cumsum(arr)[:-1]
    return np.array_split(df, sections)


def datagen(args):
    print(f"making a connection to {args.db_path}")
    conn = sqlite3.connect(args.db_path)
    # get table names
    tbl_names = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)

    # N
    dest_dir_names = []
    for i in range(args.N):
        dest_dir_name = os.path.join(args.dest_dir, f"node{i + 1}")
        os.makedirs(dest_dir_name, exist_ok=True)
        dest_dir_names.append(dest_dir_name)

    # table names
    for tbl_name in tbl_names.itertuples():
        df_data = pd.read_sql(f"SELECT * FROM {tbl_name.name}", conn)

        if len(df_data) > 100:
            # randomize the data
            df_data = df_data.sample(frac=1).reset_index(drop=True)

            if args.dist == "equal":
                df_splits = generate_equal_split(df_data, args.N)
            elif args.dist == "left":
                df_splits = generate_left_skew_split(df_data, args.N)
            elif args.dist == "right":
                df_splits = generate_right_skew_split(df_data, args.N)
            elif args.dist == "random":
                df_splits = generate_random_split(df_data, args.N)
            else:
                raise ValueError(f"Invalid dist value: {args.dist}")

            for i, df in enumerate(df_splits):
                if args.suffix == "ts":
                    suffix = datetime.now().strftime("%Y%m%d%H%M%S")
                else:
                    suffix = args.suffix

                file_name = f"{tbl_name.name.lower()}-{args.dist}"
                if suffix != "":
                    file_name += f"-{suffix}"
                if args.out_format == "csv":
                    file_path = f"{dest_dir_names[i]}/{file_name}.csv"
                    print(f"Write data to {file_path}")
                    df.to_csv(file_path, index=False)
                elif args.out_format == "parquet":
                    file_path = f"{dest_dir_names[i]}/{file_name}.parquet"
                    print(f"Write data to {file_path}")
                    df.to_parquet(file_path, index=False)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Distributed TPC H data Generator')

    parser.add_argument("--db_path", required=True, help="base dest path")
    parser.add_argument("--N", default=3, help="how many storage nodes")
    parser.add_argument("--suffix", default="ts", help='suffix for the generated file, default to timestamp, pass "" '
                                                       'for empty')
    parser.add_argument("--dist", default="equal", choices=['equal', 'left', 'right', 'random'],
                        help="the data distribution")
    parser.add_argument("--out_format", default="csv", choices=['parquet', 'csv'], help="format of the generated data")
    parser.add_argument("--dest_dir", default=os.getcwd(), help="the destination folder")

    args = parser.parse_args()

    datagen(args)
