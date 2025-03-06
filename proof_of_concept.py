import sqlite3
import numpy as np
import pandas as pd

connection = sqlite3.connect('/Users/leoni/Documents/作品集練習/taiwan_presidential_election_2024/data/taiwan_presidential_election_2024.db')
votes_by_village = pd.read_sql('''SELECT * FROM votes_by_village;''', con = connection)
connection.close()
#vector_a : 全國-三位後選人得票率
total_votes = votes_by_village['sum_votes'].sum()
country_percentage = votes_by_village.groupby('number')['sum_votes'].sum() / total_votes
vector_a = country_percentage.values #Series轉ndarray
#pivot_df : 各里-三位後選人得票率的df
groupby_variables = ['county', 'town', 'village']
village_total_votes = votes_by_village.groupby(groupby_variables)['sum_votes'].sum() #各鄉鎮總投票數（所有候選人得票數總和）
merged = pd.merge(votes_by_village, village_total_votes, left_on = groupby_variables, right_on = groupby_variables, how = 'left')
merged['village_percentage'] = merged['sum_votes_x'] / merged['sum_votes_y']
merged = merged[['county', 'town', 'village', 'number', 'village_percentage']]
pivot_df = merged.pivot(index = ['county', 'town', 'village'], columns = 'number', values = 'village_percentage').reset_index() #轉置，並加入索引行
pivot_df = pivot_df.rename_axis(None, axis = 1)#移除 DataFrame 欄位的索引名稱
#計算餘弦相似度
cosine_similarities = []
length_vector_a = pow((vector_a ** 2).sum(), 0.5)
for row in pivot_df.iterrows():
    vector_bi = np.array([row[1][1], row[1][2], row[1][3]])
    vector_a_dot_vector_bi = np.dot(vector_a, vector_bi)
    length_vector_bi = pow((vector_bi ** 2).sum(), 0.5)
    cosine_similarity = vector_a_dot_vector_bi / (length_vector_a * length_vector_bi)
    cosine_similarities.append(cosine_similarity)
#建立最後的DataFrame:cosine_similarity_df
cosine_similarity_df = pivot_df.iloc[:, :]
cosine_similarity_df['cosine_similarity'] = cosine_similarities
cosine_similarity_df = cosine_similarity_df.sort_values(['cosine_similarity', 'county', 'town', 'village'],
                                                        ascending=[False, True, True, True])#條件cosine_similarity是遞減
cosine_similarity_df = cosine_similarity_df.reset_index(drop=True).reset_index()#先丟掉原本的index，再重新排序
cosine_similarity_df['index'] = cosine_similarity_df['index'] + 1 #調整編號從1開始
column_names_to_revise = {
    'index': 'rank',
    1: 'candidate_1',
    2: 'candidate_2',
    3: 'candidate_3'
}
cosine_similarity_df = cosine_similarity_df.rename(columns=column_names_to_revise)
#篩選指定村鄰里
def filter_county_town_village(df, county_name:str, town_name:str, village_name:str):
    county_condition = df['county'] == county_name
    town_condition = df['town'] == town_name
    village_condition = df['village'] == village_name
    return df[county_condition & town_condition & village_condition]

