from cgi import test
import pandas as pd

df = pd.DataFrame({
    'Col 1': [30,40,50,60],
    'Col 2': [23,35,65,45],
    'Col 3': [85,87,90,89],

},index=["A","B","C","D"])

print("Initial DataFrame:")
print(df,"\n")

scaled_df=df.applymap(lambda a: a*10)

print("Scaled DataFrame:")
print(scaled_df,"\n")