import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np

df = pd.read_csv('/Users/lucy/BDSE30MP/RAW/全國交通事故資料/SIX_CITY_ALL.csv',encoding='UTF-8-sig', header=0,index_col=False).replace('\\N',None) #SQL匯出時會把空值補為\N，要取代

# 對 'OBJ_AGE' 欄位的數值清整
df['OBJ_AGE'].astype(int)
bins_age = [0, 18, 40, 65, np.inf]
labels_age = ['少年', '青年', '中年', '老年']
df['OBJ_AGE_CATEGORICAL'] = pd.cut(df['OBJ_AGE'].astype(int), bins=bins_age, labels=labels_age, right=False, include_lowest=True)
df.loc[df['OBJ_AGE'] < 0, 'OBJ_AGE'] = np.nan
df['OBJ_AGE'] = df['OBJ_AGE_CATEGORICAL']
df = df.drop(columns=['OBJ_AGE_CATEGORICAL'])

# 對 'SPEED_LIMIT' 欄位的數值清整

df.loc[(df['SPEED_LIMIT'] > 0) & (df['SPEED_LIMIT'] < 10), 'SPEED_LIMIT'] = df.loc[(df['SPEED_LIMIT'] > 0) & (df['SPEED_LIMIT'] < 10), 'SPEED_LIMIT'].astype(int) * 10
df.loc[(df['SPEED_LIMIT'] >= 30) & (df['SPEED_LIMIT'] < 110), 'SPEED_LIMIT'] = (df['SPEED_LIMIT'] // 10) * 10
df.loc[(df['SPEED_LIMIT'] > 110) & (df['SPEED_LIMIT'] < 200), 'SPEED_LIMIT'] = ((df['SPEED_LIMIT'] % 100) // 10) * 10
df.loc[df['SPEED_LIMIT'] > 199, 'SPEED_LIMIT'] = ((df['SPEED_LIMIT'] // 100)*10).astype(int)
df.loc[(df['SPEED_LIMIT'] >= 10) & (df['SPEED_LIMIT'] < 30) & ~df['SPEED_LIMIT'].isin([10, 15, 20, 25]), 'SPEED_LIMIT'] = None
df.loc[df['SPEED_LIMIT'] == 0, 'SPEED_LIMIT'] = None


#當事者順位清整
df['ACCIDENT_OBJ_ORDER']=df['ACCIDENT_OBJ_ORDER'].astype(int)

df.columns
df.info()
df.head()

#檢查
columns_to_check = ["SPEED_LIMIT"]

for column in columns_to_check:
    unique_values = df[column].unique()
    print(f"Column '{column}': {unique_values}")
    
#匯出CSV
#df.to_csv('SIX_CITY_V2.csv',encoding='utf-8',index=False)
