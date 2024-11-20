import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def load_tweets(filename):
    df = pd.read_csv(f"/home/rekdat/tweets-data/transformed/{filename}.csv")
    df['date'] = pd.to_datetime(df['date'])
    return df

def load_json(filename):
    df = pd.read_json(f"/home/rekdat/pinterest-data/transformed/transformed_{filename}.json", typ='series')
    df = df.reset_index()
    df.columns = ['date', 'growth_normalized']
    df['date'] = pd.to_datetime(df['date'])
    df['growth_normalized'] = df['growth_normalized'].astype(float)
    return df

def integrate_data(df1, df2, filename):
    merged = pd.merge(df1, df2, on="date", how="outer", suffixes=("_tweet", "_pinterest"))
    # Replace NaN values with 0 for missing weeks
    merged["growth_normalized_tweet"] = merged["growth_normalized_tweet"].fillna(0)
    merged["growth_normalized_pinterest"] = merged["growth_normalized_pinterest"].fillna(0)
    
    # Aggregate the values by summing
    merged["total_growth"] = merged["growth_normalized_tweet"] + merged["growth_normalized_pinterest"]
    
    # Sort by week to maintain chronological order
    merged = merged.sort_values(by="date")
    
    # Drop unnecessary columns if desired
    merged = merged[["date", "total_growth"]]
    merged["total_growth"] = merged["total_growth"].clip(lower=0, upper=100)

    # Save to csv
    merged.to_csv(f"/home/rekdat/combined-data/{filename}.csv", index=False)