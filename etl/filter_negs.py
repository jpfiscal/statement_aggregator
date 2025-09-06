import pandas as pd
import math

def filter_cr(data):
    for x in data.index:
        if data.loc[x, "amount"] <= 0 or math.isnan(data.loc[x,"amount"]):
            data.drop(x, inplace = True)
    return data