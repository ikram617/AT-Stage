import pandas as pd


dataset = pd.read_csv("../data/dataset_final.csv")

print(dataset.head().to_string())