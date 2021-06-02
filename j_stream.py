import os, sys, glob
import pandas as pd 
from pathlib import Path
import datetime
import os

##元データのcsvファイルを取得
df_1 = pd.read_csv('j_stream.csv',header = None)
M = df_1.shape[1]
##0列目と3列目から(N-1)列目を取得すればよい．0行目のみを抽出して格納するdfと3行目から(N-1)行目を抽出して格納するdfを作る

df_2 = df_1.iloc[:,0]
df_3 = df_1.iloc[:,3:M]

df_4 = pd.concat([df_2,df_3],axis= 1)
##ここまでで必要な列の抽出が終了.indexをとってきてそこからデータ抽出に必要なデータの読み込みを行う

series_index = df_4.iloc[0,:] ##indexを抽出した
N = df_4.shape[0] ##元データのdfの行数を取得した．

COLUMNS = ['user_id','dcf_code','created_datetime','answer_id','survay_id','branch_id','question_id','question_text','choice_id','answer_text','mlliveid']
df_5 =  pd.DataFrame(index=[], columns=COLUMNS)
##ここへデータをappendしていく
series_append = pd.Series(['','',datetime.date.today(),'','','','','','','',''],index = COLUMNS)

##for文を回すのに必要なリストの長さを取得する
size_of_series_index = len(series_index)
size_of_series_index = int(size_of_series_index)

print(series_index)
print(size_of_series_index)
##質問番号を格納したリストを作る
List_question_id = [0]*(size_of_series_index -3)

##質問番号をカウントする変数を用意する
count_question_id = 0

for j in range(3,size_of_series_index):
    if(pd.isnull(series_index[j])==False):
        count_question_id = count_question_id + 1
        List_question_id[j-3] = count_question_id
    else:
        List_question_id[j-3] = count_question_id

List_question_text_pre = ['']*(count_question_id)

count = 3

for j in range(3,size_of_series_index):
    if(pd.isnull(series_index[j])==False):
        List_question_text_pre[count-3] = series_index[j]
        count = count + 1

List_question_text = ['']*(size_of_series_index - 3)

count = 0

for i in range(3,size_of_series_index):
    if(pd.isnull(series_index[i])==False):
        List_question_text[i-3] = List_question_text_pre[count]
        count = count + 1
    else:
        List_question_text[i-3] = List_question_text_pre[count]

##dfの列サイズ自体が別途に必要なので宣言して格納する
M = df_4.shape[1]

for i in range(2,N):
    series_append['user_id'] = df_4.iat[i,M-1]
    series_append['created_datetime'] = df_4.iat[i,0]
    for j in range(3,M-2):
        series_append['question_id'] = List_question_id[j-3]
        series_append['question_text'] = List_question_text[j-3]
        series_append['answer_text'] = df_4.iat[i,j]
        df_5 = df_5.append(series_append,ignore_index = True)

df_5.to_csv('jstream_processed.csv',index = False,encoding = 'utf-8')