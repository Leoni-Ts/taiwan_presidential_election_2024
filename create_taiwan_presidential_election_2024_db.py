import pandas as pd
import os
import re
import sqlite3

class CreateTaiwanPresidentialElection2024DB:
    def __init__(self):
        #取得22個縣市的名稱
        file_names = os.listdir("data")
        county_names = []
        for file_name in file_names:
            if ".xlsx" in file_name:
                file_name_split = re.split("\\(|\\)", file_name)
                county_names.append(file_name_split[1])
        self.county_names = county_names

    def tidy_county_dataframe(self, county_name:str):
        file_path = f'/Users/leoni/Documents/作品集練習/taiwan_presidential_election_2024/data/總統-A05-4-候選人得票數一覽表-各投開票所({county_name}).xlsx'
        df = pd.read_excel(file_path, skiprows=[0, 3, 4])
        df = df.iloc[:, :6]
        candidates_info = df.iloc[0, 3:].values.tolist()
        df.columns = ['town', 'village', 'polling_place'] + candidates_info
        df.loc[:, 'town'] = df['town'].ffill()#填補未定義值
        df.loc[:, 'town'] = df['town'].str.strip()#去掉字串頭尾中多餘的空白
        df = df.dropna()
        df['polling_place'] = df['polling_place'].astype(int)
        id_variables = ['town', 'village', 'polling_place']
        melted_df = pd.melt(df, id_vars= id_variables, var_name = 'candidate_info', value_name = 'votes')#轉置資料框
        melted_df['county'] = county_name
        return melted_df

    def concat_county_dataframe(self):
        #整合22個縣市的資料框
        country_df = pd.DataFrame()
        for county_name in self.county_names:
            county_df = self.tidy_county_dataframe(county_name)
            country_df = pd.concat([country_df, county_df])
        country_df = country_df.reset_index(drop = True)
        #候選人編號和名字拆成兩個list
        numbers, candidates = [], []
        for elem in country_df['candidate_info'].str.split('\n'):
            number = re.sub('\\(|\\)', '', elem[0])
            numbers.append(int(number))
            candidate = elem[1] + '/' + elem[2]
            candidates.append(candidate)
        ##重組資料框 #1.把所需資料全放入presidential_votes中
        presidential_votes = country_df.loc[:,['county', 'town', 'village', 'polling_place']]
        presidential_votes['number'] = numbers
        presidential_votes['candidate'] = candidates
        presidential_votes['votes'] = country_df['votes'].values
        return presidential_votes

    def create_database(self):
        presidential_votes = self.concat_county_dataframe()
        #2.製作第一個DataFrame:polling_places_df
        polling_places_df = presidential_votes.groupby(['county', 'town', 'village', 'polling_place']).count().reset_index() #將polling_places_df用投票所分組（原本會有每個候選人在各投票所得票數，投票所會重複，這裡只要找出不重複的所有投票所），reset_index()可讓分組的一句從索引值回到普通欄位
        polling_places_df = polling_places_df[['county', 'town', 'village', 'polling_place']] #只要投票所欄位
        polling_places_df = polling_places_df.reset_index() #加入索引值
        polling_places_df['index'] = polling_places_df['index'] + 1 #讓索引值從1開始編號
        polling_places_df = polling_places_df.rename(columns = {'index':'id'}) #改索引值欄位名稱
        #3.製作第二個DataFrame:candidates_df
        candidates_df = presidential_votes.groupby(['number', 'candidate']).count().reset_index()
        candidates_df = candidates_df[['number', 'candidate']]
        candidates_df = candidates_df.reset_index()
        candidates_df['index'] = candidates_df['index'] + 1
        candidates_df = candidates_df.rename(columns = {'index':'id'})
        #4. 製作第三個DataFrame:votes_df
        join_keys = ['county', 'town', 'village', 'polling_place']
        votes_df = pd.merge(presidential_votes, polling_places_df, left_on=join_keys, right_on=join_keys, how='left')#把polling_places_df的id欄位放入presidential_votes中
        votes_df = votes_df[['id', 'number', 'votes']]
        votes_df = votes_df.rename(columns={'id': 'polling_place_id', 'number': 'candidate_id'})
        connection = sqlite3.connect('/Users/leoni/Documents/作品集練習/taiwan_presidential_election_2024/data/taiwan_presidential_election_2024.db')
        polling_places_df.to_sql('polling_places', con=connection, if_exists='replace', index=False)
        candidates_df.to_sql('candidates', con=connection, if_exists='replace', index=False)
        votes_df.to_sql('votes', con=connection, if_exists='replace', index=False)
        cur = connection.cursor()#建立檢視表
        drop_view_sql = """
        DROP VIEW IF EXISTS votes_by_village;
        """
        create_view_sql = """
        CREATE VIEW votes_by_village AS
        SELECT polling_places.county,
               polling_places.town,
               polling_places.village,
               candidates.number,
               candidates.candidate,
               SUM(votes.votes) AS sum_votes
          FROM votes
          LEFT JOIN polling_places
            ON votes.polling_place_id = polling_places.id
          LEFT JOIN candidates
            ON votes.candidate_id = candidates.id
         GROUP BY polling_places.county,
                  polling_places.town,
                  polling_places.village,
                  candidates.id;
        """
        cur.execute(drop_view_sql)
        cur.execute(create_view_sql)
        connection.close()
create_taiwan_presidential_election_2024_db = CreateTaiwanPresidentialElection2024DB()
create_taiwan_presidential_election_2024_db.create_database()





