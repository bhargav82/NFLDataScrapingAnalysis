import csv
import requests
from bs4 import BeautifulSoup, Comment
import time
import random
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt


# df = pd.read_csv("wsa_RecordsData.csv")

# teams = df.groupby("Team")


# for name, group in teams:
#     print(f"Group: {name}")
#     print(group)

#     plt.figure(figsize = (6.4, 4.8))

#     for i in range(len(group)):
#         year = group.iloc[i]['Year']
#         plt.text(group.iloc[i]['Percent_Retention'], group.iloc[i]['Win_Percentage'], str(year), fontsize=8, ha='center')


    
    
#     plt.scatter(group['Percent_Retention'], group['Win_Percentage'])
#     plt.
#     plt.xlabel("Player Retention Percentage")
#     plt.ylabel("Win Percentage")
#     plt.title(name)
#     plt.show()


nfl_teams = {
    "Buffalo Bills" : "buf",
    "Miami Dolphins" : "mia",
    "New York Jets" : "nyj",
    "New England Patriots" : "nwe",
    "Baltimore Ravens" : "rav",
    "Cincinnati Bengals" : "cin",
    "Cleveland Browns" : "cle",
    "Pittsburgh Steelers" : "pit",
    "Houston Texans" : "htx",
    "Indianapolis Colts" : "clt",
    "Jacksonville Jaguars" : "jax",
    "Tennessee Titans" : "oti",
    "Denver Broncos" : "den",
    "Kansas City Chiefs" : "kan",
    "Los Angeles Chargers" : "sdg",
    "Las Vegas Raiders" : "rai",
    "Dallas Cowboys" : "dal",
    "New York Giants" : "nyg",
    "Philadelphia Eagles" : "phi",
    "Washington Commanders" : "was",
    "Chicago Bears" : "chi",
    "Detroit Lions" : "det",
    "Green Bay Packers" : "gnb",
    "Minnesota Vikings" : "min",
    "Atlanta Falcons" : "atl",
    "Carolina Panthers" : "car",
    "New Orleans Saints" : "nor",
    "Tampa Bay Buccaneers" : "tam",
    "Arizona Cardinals" : "crd",
    "Los Angeles Rams" : "ram",
    "San Francisco 49ers" : "sfo",
    "Seattle Seahawks" : "sea"
    
}

previous_years = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024"]
all_teams_rosters = {}


for year in previous_years:

    all_teams_rosters[year] = {}

    for team_name, team_id in nfl_teams.items():
        print(f"Fetching Roster data for {team_name}")
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        })

        url = (f"https://www.pro-football-reference.com/teams/{team_id}/{year}_roster.htm") 
        url_request = session.get(url)
        time.sleep(random.uniform(1, 3))

        soup = BeautifulSoup(url_request.text, "html.parser")
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))

        roster_table = None
    
        for comment in comments:
            if "table" in comment:
                comment_soup = BeautifulSoup(comment, "html.parser")
                roster_table = comment_soup.find("table", {"id" : "roster"})
                if roster_table:
                    break


        team_roster = []

        if roster_table:
            
            rows = roster_table.find("tbody").find_all("tr")

            for row in rows:
                columns = row.find_all('td')
                if columns:
                    player_data = {}
                    if columns[0].find('a'):
                        name = columns[0].find('a').text.strip()
                    else:
                        name = columns[0].text.strip()
                    

                    if len(columns) > 10:
                        player_value = columns[10].text.strip()
                    else:
                        player_value = "N/A"

                    player_data[name] = player_value
                    team_roster.append(player_data)

            
        

        all_teams_rosters[year][team_name] = team_roster






# with open("wsa_data.csv", 'w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerow(["Year", "Team", "Player", "Value"])
#     for year, teams in all_teams_rosters.items():
#         for team, roster in teams.items():
#             for player in roster:
#                 for name, value in player.items():
#                     writer.writerow([year, team, name, value])
  


config = {
  'user': 'wsa',
  'host': '34.68.250.121',
  'database': 'Tutorials-Winter2025',
  'password': 'LeBron>MJ!'
}

try:
    cnx = mysql.connector.connect(**config)
    print("Successfully connected to database")
except mysql.connector.Error as err:
    print(err)


cursor = cnx.cursor(buffered = True)

print("Starting the data insertion process")

create_table_query = """
CREATE TABLE IF NOT EXISTS nfl_team_success (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year VARCHAR(4),
    team_name VARCHAR(50),
    player_name VARCHAR(100),
    player_value VARCHAR(50)
)
"""

try:
    cursor.execute(create_table_query)
    cnx.commit()
    print("Table created or already exists.")
except mysql.connector.Error as err:
    print(f"Error when creating table: {err}")

insert_query = """
INSERT INTO nfl_team_success (year, team_name, player_name, player_value)
VALUES (%s, %s, %s, %s)
"""

inserted_rows = 0

for year, teams in all_teams_rosters.items():
    for team, roster in teams.items():
        for player in roster:
            for name, value in player.items():
                try:
                    cursor.execute(insert_query, (year, team, name, value))
                    inserted_rows += 1

                    if inserted_rows %100 == 0:
                        cnx.commit()
                        print(f"Inserted {inserted_rows} rows so far")
                except mysql.connector.Error as err:
                    print(f"Error inserting data: {err}")


print("Inserted complete")


queryPlayerData = "SELECT * FROM `Tutorials-Winter2025`.nfl_team_success;"
df_playerData = pd.read_sql(queryPlayerData, cnx)

queryTeamData = "SELECT * FROM `Tutorials-Winter2025`.NFL_Team_Records_10_to_24;"
df_teamData = pd.read_sql(queryTeamData, cnx)



cnx.commit()
cursor.close()
cnx.close()



# SQL Query to get all rows
# SELECT * FROM `Tutorials-Winter2025`.nfl_team_success;