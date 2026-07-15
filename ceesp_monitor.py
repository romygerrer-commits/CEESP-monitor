import pandas as pd

url = "https://public.tableau.com/app/profile/has8400/viz/Contributionpatient/Tableaudebord5.csv"

df = pd.read_csv(url)
print(df.head())
