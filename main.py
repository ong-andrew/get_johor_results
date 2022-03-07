import requests
import random
import json
import pandas as pd
import pygsheets
from discord_webhook import DiscordWebhook


#DICTIONARIES
consti = {'14001':'Buloh Kasap','14002':'Jementah','14103':'Pemanis','14104':'Kemelah','14205':'Tenang','14206':'Bekok','14307':'Bukit Kepong','14308':'Bukit Pasir','14409':'Gambir','14410':'Tangkak','14411':'Serom','14512':'Bentayan','14513':'Simpang Jeram','14514':'Bukit Naning','14615':'Maharani','14616':'Sungai Balang','14717':'Semerah','14718':'Sri Medan','14819':'Yong Peng','14820':'Semarang','14921':'Parit Yaani','14922':'Parit Raja','15023':'Penggaram','15024':'Senggarang','15025':'Rengit','15126':'Machap','15127':'Layang-layang','15228':'Mengkibol','15229':'Mahkota','15330':'Paloh','15331':'Kahang','15432':'Endau','15433':'Tenggaroh','15534':'Panti','15535':'Pasir Raja','15636':'Sedili','15637':'Johor Lama','15738':'Penawar','15739':'Tanjung Surat','15840':'Tiram','15841':'Puteri Wangsa','15942':'Johor Jaya','15943':'Permas','16044':'Larkin','16045':'Stulang','16146':'Perling','16147':'Kempas','16248':'Skudai','16249':'Kota Iskandar','16350':'Bukit Permai','16351':'Bukit Batu','16352':'Senai','16453':'Benut','16454':'Pulai Sebatang','16555':'Pekan Nanas','16556':'Kukup'}
parties = {0: 'TIADA', 1: 'BN', 2: 'PAS', 3: 'DAP', 4: 'BERJASA', 5: 'SNAP', 6: 'PBB', 7: 'KIMMA', 8: 'PASOK', 9: 'PRM', 10: 'PBDS', 11: 'PBS', 12: 'AKAR', 13: 'MOMOGUN', 14: 'KEADILAN', 15: 'STAR', 16: 'UPKO', 17: 'AKIM', 18: 'MDP', 19: 'PPM', 20: 'BEBAS', 21: 'BERSEKUTU', 22: 'SETIA', 23: 'CCC', 24: 'GERAKAN', 25: 'PPP', 26: 'LDP', 27: 'SAPP', 28: 'AMIPF', 29: 'KEMAJUAN', 30: 'MWP', 31: 'UMNO', 32: 'MCA', 33: 'MIC', 34: 'SUPP', 35: 'PBRS', 36: 'SPDP', 37: 'PKR', 38: 'PRS', 39: 'PEJUANG', 40: 'PERMAS', 41: 'PFP', 42: 'BERJAYA', 43: 'NEWGEN', 44: 'PN', 45: 'PSRM', 46: 'GAGASAN', 47: 'ASPIRASI', 48: 'MUPP', 49: 'PBDSB', 50: 'PH', 51: 'GPS', 52: 'USNO', 53: 'PUTRA', 54: 'PSB', 55: 'PUSAKA', 56: 'PAJAR', 57: 'SAPO', 58: 'KITA', 59: 'PEKEMAS', 60: 'BISAMAH', 61: 'IPPP', 62: 'MUDA', 63: 'PER', 64: 'PESAKA', 65: 'PR', 66: 'PPP', 67: 'UMCO', 68: 'SCA', 69: 'IMAN', 70: 'SEDAR', 71: 'PNRS', 72: 'UMAT', 73: 'UPP', 74: 'M.M.S.P', 75: 'KITA', 76: 'BERSAMA', 77: 'PCM', 78: 'PBM', 79: 'PSM', 80: 'MCC', 81: 'AMANAH', 82: 'PPRS', 83: 'ANAKNEGERI', 84: 'PEACE', 85: 'TERAS', 86: 'PKS', 87: 'SAPU', 88: 'PBDSB', 89: 'PAP', 90: 'PCS', 91: 'WARISAN', 92: 'PPBM', 93: 'SOLIDARITI', 94: 'HR', 95: 'PBK', 96: 'IKATAN', 97: 'MU', 98: 'PERPADUAN', 99: 'SPP'}

#Get raw data
rawak = random.randint(0,100000)
url = "https://dashboard.spr.gov.my/js/penamaan.js?"+str(rawak)
headers = {'user-agent': 'my-agent/1.0.1'}
site = requests.get(url, headers=headers).text

#Processing
cleaned = site.replace("var dataPenamaan = ","{\"johor\": ").replace(";","}") #Turn data into json

#loads the pretty json
raw_data = json.loads(cleaned) 

#pd.json_normalize flatters json https://bit.ly/3sr2ufq
df = pd.json_normalize(raw_data, record_path =['johor']) 

#constituency name as index | rename column and index | set column order
pivot = df.pivot_table(values='ju',index='kid',columns='pid',fill_value=0,aggfunc='sum')
renamed = pivot.rename(index=consti,columns=parties,inplace=True)
pivoted = pivot[['BN','PH','PKR','MUDA','PN','PEJUANG','WARISAN','PBM','PUTRA','PSM','BEBAS']]
pivoted.to_csv('upload_this.csv')

#export to Gsheet using pygsheets
df_pivoted = pd.read_csv('upload_this.csv')
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
client = pygsheets.authorize(service_file='cred.json')
sheet = client.open("Johor 2022 tally").worksheet_by_title("OFFICIAL")
sheet.set_dataframe(df_pivoted,(0,0),copy_index=False)

#Compare old and new CSV
with open('upload_this.csv', 'r') as A, open('old.csv', 'r') as B:
    old_csv = B.readlines()
    new_csv = A.readlines()

    if old_csv == new_csv:
        print("No change")

    else:
        for line in new_csv:
            if line not in old_csv:
                print(line)
                msg = f"{line}"
                webhook = DiscordWebhook(url='https://discord.com/api/webhooks/950306623027560469/rrpm-KB-h7KjbMW-J-d_Yb8ZDzcF52qSBAeMuWdPhFs5H-_V7zaUWyrPzp1u5Zh1KAVl', content = msg)
                execute = webhook.execute()

        with open('old.csv', 'w') as data:
            data.writelines(new_csv)

