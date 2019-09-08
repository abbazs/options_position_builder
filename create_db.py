import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///data1.db", echo=False)
df = pd.read_csv("sample_data.3.csv")
df.to_sql("DATA", engine)
