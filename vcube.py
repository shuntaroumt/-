import os, sys, glob
import pandas as pd 
from pathlib import Path
import datetime
import os

df_1 = pd.read_csv('v_cube.csv',header = None)
##インデックスのサイズを取得
N = df_1.shape[1]
##0列目と3列目から(N-1)列目を取得すればよい．0行目のみを抽出して格納するdfと3行目から(N-1)行目を抽出して格納するdfを作る

df_2 = df_1.iloc[:,0]
df_3 = df_1.iloc[:,3:N]

df_4 = pd.concat([df_2,df_3],axis= 1)
##ここまでで必要な列の抽出が終了.df_4を2次元リストに格納して考えます
##df_4 の行数を取得
M = df_4.shape[0]

##df_4の列数を取得
N = df_4.shape[1]

##加工前の元データdf_4を格納するリストを作る

List_1 = []

List_1.append(df_4)
##加工後のデータを格納する空のデータフレームを作成する．

COLUMNS = ['user_id','dcf_code','created_datetime','answer_id','survay_id','branch_id','question_id','question_text','choice_id','answer_text','mlliveid']
df_5 =  pd.DataFrame(index=[], columns=COLUMNS)
##ここへデータをappendしていく
series_append = pd.Series(['','','','','','','','','','',''],index = COLUMNS)
for i in range(1,M):
    series_append['user_id'] = df_4.iat[i,0]
    for j in range(1,N):
        series_append['question_id'] = j 
        series_append['question_text'] = df_4.iat[0,j]
        series_append['answer_text'] = df_4.iat[i,j]
        df_5 = df_5.append(series_append,ignore_index = True)

df_5.to_csv('vcube_processed.csv',index = False,encoding = 'utf-8')
