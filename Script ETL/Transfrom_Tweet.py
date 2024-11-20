import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

def model_growth(df):
    '''
    Model weekly growth of tweet data
    '''

    # Take only necessary data
    filtered_df = df[['created_at', 'favorite_count', 'quote_count', 'reply_count', 'retweet_count']]
    
    # Modeling engagement by taking the mean of metrics
    filtered_df = filtered_df.copy()
    filtered_df['metrics'] = (filtered_df['favorite_count'] + filtered_df['quote_count'] + filtered_df['reply_count'] + filtered_df['retweet_count']) / 4
    filtered_df = filtered_df.drop_duplicates()
    
    # Convert timestamp column to datetime format
    filtered_df['created_at'] = pd.to_datetime(filtered_df['created_at'])
    # Group by date and topic
    filtered_df['date'] = filtered_df['created_at'].dt.date  # Extract date without time
    
    # Group metrics by date
    transformed_df = filtered_df.groupby('date')['metrics'].sum()
    transformed_df = transformed_df.reset_index()
    
    # Model weekly growth
    transformed_df['date'] = pd.to_datetime(transformed_df['date'])
    transformed_weekly = transformed_df.resample('W-Mon', on='date').sum()
    transformed_weekly['metrics_smoothed'] = transformed_weekly['metrics'].rolling(window=3).mean()
    transformed_weekly['weekly_growth_smoothed'] = transformed_weekly['metrics_smoothed'].pct_change() * 100
    transformed_weekly['weekly_growth_smoothed'] = transformed_weekly['weekly_growth_smoothed'].replace([np.inf, -np.inf], np.nan).fillna(0)

    # Normalize data
    max_value = transformed_weekly['weekly_growth_smoothed'].max()
    transformed_weekly['growth_normalized'] = (transformed_weekly['weekly_growth_smoothed'] / max_value) * 100
    transformed_weekly = transformed_weekly.reset_index()
    transformed_weekly = transformed_weekly[['date', 'growth_normalized']]
    
    return transformed_weekly

def transform_data(input_filename, output_filename):
    input_path = os.path.expanduser(f'~/tweets-data/{input_filename}.csv')  # Input file
    output_path = os.path.expanduser(f'~/tweets-data/transformed/{output_filename}.csv')  # Output file
    
    df = pd.read_csv(input_path)
    
    # Call the model_growth function
    transformed_df = model_growth(df)
    
    # # Save the result to the output path
    transformed_df.to_csv(output_path)
    print(f"Transformed data saved to {output_path}")