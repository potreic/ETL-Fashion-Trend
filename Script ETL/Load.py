import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlalchemy
from sqlalchemy import create_engine

def load_data(df, table_name):
    '''
    Load transformed dataframe to a PostgreSQL database acting as a "data warehouse" for further analysis
    '''
    # Create SQLAlchemy engine
    engine = create_engine("postgresql+psycopg2://postgres:loveisgone@localhost:5432/fashiondb")
    
    # Write DataFrame to PostgreSQL
    df.to_sql(table_name, engine, if_exists='append', index=False)

def plot_data(df, keyword):
    df['date'] = pd.to_datetime(df['date'])
    plt.figure(figsize=(10, 6))
    plt.plot(df['date'], df['total_growth'], marker='o')
    plt.title('Weekly Growth of Metrics')
    plt.xlabel('Date')
    plt.ylabel('Weekly Growth (%)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.show()
    plt.savefig(f'/home/rekdat/combined-data/output{keyword}.jpg')