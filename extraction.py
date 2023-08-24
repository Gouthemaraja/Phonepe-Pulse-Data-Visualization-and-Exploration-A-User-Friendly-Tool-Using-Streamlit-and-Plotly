#importing packages 

import pandas as pd 
import os 
import json 

#dirctories
dic=r'pulse\data\aggregated\transaction\country\india'
dic_state=r'pulse\data\aggregated\transaction\country\india\state'
dic_user_india=r'pulse\data\aggregated\user\country\india'
dic_user_india_state = r"pulse\data\aggregated\user\country\india\state"
dic_map_trans_india = r'pulse\data\map\transaction\hover\country\india'
dic_map_trans_india_state = r'pulse\data\map\transaction\hover\country\india\state'
dic_map_user_india  = r'pulse\data\map\user\hover\country\india'
dic_map_user_india_state = r"pulse\data\map\user\hover\country\india\state"
dic_top_trans_india = r'pulse\data\top\transaction\country\india'
dic_top_trans_india_state = r'pulse\data\top\transaction\country\india\state'
dic_top_user_india = r'pulse\data\top\user\country\india'
dic_top_user_india_state = r'pulse\data\top\user\country\india\state'

#function for return datafame form aggregated transaction of india 

def agg_trans_india(path_agg_trans_india):
    dic = path_agg_trans_india
    df1 = pd.DataFrame()
    for year in os.listdir(dic):
        if year in ["2018", "2019", "2020", "2021","2022"]:
            year_path = os.path.join(dic, year)  # Use 'year' instead of 'x'

            for q in os.listdir(year_path):
                year_file = os.path.join(year_path, q)
                with open(year_file) as json_file:
                    data_at = json.load(json_file)   
                transactionData = data_at["data"]["transactionData"]
                res_list = []
                for transaction in transactionData:
                    name = str(transaction["name"])
                    count = int(transaction["paymentInstruments"][0]["count"])
                    amount = float(transaction["paymentInstruments"][0]["amount"])
                    qyear = int(year)
                    if q == "1.json":
                        quater = "Q1"
                    elif q == "2.json":
                        quater = "Q2"
                    elif q == "3.json":
                        quater = "Q3"
                    elif q == "4.json":
                        quater = "Q4"   
                    row = {"payment category": name, "qyear": qyear, "quater": quater, "count": count, "amount": amount}
                    res_list.append(row)
                df_temp = pd.DataFrame(res_list)
                df1 = pd.concat([df1, df_temp], ignore_index=True)
    return df1


# print(agg_trans_india(r'pulse\data\aggregated\transaction\country\india'))

#function for return datafame form aggregated transaction of states

def agg_trans_india_state(path_agg_trans_india_state):
    dic_state=path_agg_trans_india_state
    df2 = pd.DataFrame()
    for state in os.listdir(dic_state ):
        state_path = os.path.join(dic_state, state)
        # print(state_path)
        for year in os.listdir(state_path):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(state_path, year)  
                # print(year_path)
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_at = json.load(json_file)   
                    transactionData = data_at["data"]["transactionData"]
                    res_list = []
                    for transaction in transactionData:
                        name = str(transaction["name"])
                        count = int(transaction["paymentInstruments"][0]["count"])
                        amount = float(transaction["paymentInstruments"][0]["amount"])
                        qyear = int(year)
                        state = state
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state":state,"payment category": name, "qyear": qyear, "quater": quater, "count": count, "amount": amount}
                        res_list.append(row)
                    df_temp = pd.DataFrame(res_list)
                    df2 = pd.concat([df2, df_temp], ignore_index=True)
    return df2


#function for return datafame form aggregated users of india 

def agg_user_india(path_agg_user_india):
    dic_user_india = path_agg_user_india
    df3 = pd.DataFrame()
    for year in os.listdir(dic_user_india):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(dic_user_india, year)  

                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    with open(year_file) as json_file:
                        data_au = json.load(json_file)
                    registeredUsers = data_au["data"]["aggregated"]["registeredUsers"]
                    appOpens = data_au["data"]["aggregated"]["appOpens"] 
                    usersByDevice = data_au["data"]["usersByDevice"]
                    res_list = []
                    try:
                        for user in usersByDevice:
                            brand = str(user["brand"])
                            count = int(user["count"])
                            percentage = float(user["percentage"])
                            qyear = int(year)
                            if q == "1.json":
                                quater = "Q1"
                            elif q == "2.json":
                                quater = "Q2"
                            elif q == "3.json":
                                quater = "Q3"
                            elif q == "4.json":
                                quater = "Q4"   
                            row = {"registeredUsers": registeredUsers, "appOpens": appOpens, "brand": brand, "count":count,"percentage":percentage,"qyear": qyear, "quater": quater}
                            res_list.append(row)
                    except:
                        brand = "combined"
                        count = 0
                        percentage = 0
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"registeredUsers": registeredUsers, "appOpens": appOpens, "brand": brand, "count":count,"percentage":percentage,"qyear": qyear, "quater": quater}
                        res_list.append(row)
                    df_temp = pd.DataFrame(res_list)
                    df3 = pd.concat([df3, df_temp], ignore_index=True)
    return df3


#function for return datafame form aggregated users of states

def agg_user_india_state(path_agg_user_india_state):
    dic_user_india_state = path_agg_user_india_state
    df4 = pd.DataFrame()
    for state in os.listdir(dic_user_india_state):
        state_path = os.path.join(dic_user_india_state, state)
        for year in os.listdir(state_path):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(state_path, year)  
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    with open(year_file) as json_file:
                        data_au = json.load(json_file)
                    state = state
                    registeredUsers = data_au["data"]["aggregated"]["registeredUsers"]
                    appOpens = data_au["data"]["aggregated"]["appOpens"] 
                    usersByDevice = data_au["data"]["usersByDevice"]
                    res_list = []
                    # print(usersByDevice)
                    try:
                        for user in usersByDevice:
                            brand = str(user["brand"])
                            count = int(user["count"])
                            percentage = float(user["percentage"])
                            qyear = int(year)
                            if q == "1.json":
                                quater = "Q1"
                            elif q == "2.json":
                                quater = "Q2"
                            elif q == "3.json":
                                quater = "Q3"
                            elif q == "4.json":
                                quater = "Q4"   
                            row = {"state":state,"registeredUsers": registeredUsers, "appOpens": appOpens, "brand": brand, "count":count,"percentage":percentage,"qyear": qyear, "quater": quater}
                            res_list.append(row)
                    except:
                        brand = 0
                        count = 0
                        percentage = 0
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state":state,"registeredUsers": registeredUsers, "appOpens": appOpens, "brand": brand, "count":count,"percentage":percentage,"qyear": qyear, "quater": quater}
                        res_list.append(row)

                    df_temp = pd.DataFrame(res_list)
                    df4 = pd.concat([df4, df_temp], ignore_index=True)
    return df4


#function for return datafame from map transaction india

def map_trans_india(path_map_trans_india):
    dic_map_trans_india = path_map_trans_india
    df5 = pd.DataFrame()
    for year in os.listdir(dic_map_trans_india):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(dic_map_trans_india, year)  

                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    with open(year_file) as json_file:
                        data_mt = json.load(json_file)   
                    hoverDataList = data_mt["data"]["hoverDataList"]
                    res_list = []
                    for data in hoverDataList:
                        state = str(data["name"])
                        count = int(data["metric"][0]["count"])
                        amount = float(data["metric"][0]["amount"])
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state": state, "count": count, "amount": amount, "qyear": qyear, "quater": quater}
                        res_list.append(row)
                    df_temp = pd.DataFrame(res_list)
                    df5 = pd.concat([df5, df_temp], ignore_index=True)
    return df5


#function for return datafame from map transaction states

def map_trans_india_states(path_map_trans_state):
    dic_map_trans_india_state = path_map_trans_state
    df6 = pd.DataFrame()
    for state in os.listdir(dic_map_trans_india_state):
        state_path = os.path.join(dic_map_trans_india_state, state)
        # print(state_path)
        for year in os.listdir(state_path):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(state_path, year)  
                # print(year_path)
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_mts = json.load(json_file)
                    # print(data_mts)   
                    hoverDataList = data_mts["data"]["hoverDataList"]
                    res_list = []
                    for data in hoverDataList:
                        state_zone = str(data["name"])
                        count = int(data["metric"][0]["count"])
                        amount = float(data["metric"][0]["amount"])
                        qyear = int(year)
                        state = state
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state":state,"zone":state_zone,"count": count, "amount": amount, "qyear": qyear, "quater": quater}
                        res_list.append(row)
                    df_temp = pd.DataFrame(res_list)
                    df6 = pd.concat([df6, df_temp], ignore_index=True)
    return df6


#function for return datafame from map user india 

def map_user_india(path_map_user_india):
    dic_map_user_india = path_map_user_india
    df7 = pd.DataFrame()
    for year in os.listdir(dic_map_user_india):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(dic_map_user_india, year)
                # print(year_path) 
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_mt = json.load(json_file)   
                    hoverData = data_mt["data"]["hoverData"]
                    # print(hoverData)
                    res_list = []
                    for data,values in hoverData.items():
                        state = data
                        dict = values
                        # print(state)
                        # print(dict)
                        registeredUsers = dict["registeredUsers"]
                        appOpens = dict["appOpens"]
                    
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state": state, "registeredUsers": registeredUsers, "appOpens": appOpens, "qyear": qyear,"quater": quater}
                        res_list.append(row)
                    df_temp = pd.DataFrame(res_list)
                    df7 = pd.concat([df7, df_temp], ignore_index=True)
    return df7


#function for return datafame from map user state
def map_user_india_states(path_map_user_india_state):
    dic_map_user_india_state = path_map_user_india_state
    df8 = pd.DataFrame()
    for state in os.listdir(dic_map_user_india_state):
        state_path = os.path.join(dic_map_user_india_state, state)
        # print(state_path)
        for year in os.listdir(state_path):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(state_path, year)  
                # print(year_path)
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_mts = json.load(json_file)
                    # print(data_mts)   
                    hoverData = data_mts["data"]["hoverData"]
                    res_list = []
                    
                    for data,values in hoverData.items():
                        state = state
                        zone = data
                        dict = values
                        # print(state)
                        # print(dict)
                        registeredUsers = dict["registeredUsers"]
                        appOpens = dict["appOpens"]
                        
                        qyear = int(year)
                        state = state
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state": state,"zone":zone, "registeredUsers": registeredUsers, "appOpens": appOpens, "qyear": qyear,"quater": quater}
                        res_list.append(row)
                    df_temp = pd.DataFrame(res_list)
                    df8= pd.concat([df8, df_temp], ignore_index=True)
    return df8
    

#function for return datafame from top trans india 

def top_trans_india(path_top_trans_india):
    dic_top_trans_india = path_top_trans_india
    dfs = pd.DataFrame()
    dfd = pd.DataFrame()
    dfpc = pd.DataFrame()
    for year in os.listdir(dic_top_trans_india):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(dic_top_trans_india, year)
                # print(year_path) 
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_mt = json.load(json_file)   
                    states = data_mt["data"]["states"]
                    districts = data_mt["data"]["districts"]
                    pincodes = data_mt["data"]["pincodes"]
                    # print(hoverData)
                    for states in states:
                        # print(state)
                        state = states["entityName"]
                        count = states["metric"]["count"]
                        amount = states["metric"]["amount"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state": state, "count": count,"amount":amount, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfs = pd.concat([dfs, df_temp], ignore_index=True)
                    for districts in districts:
                        district = districts["entityName"]
                        count = districts["metric"]["count"]
                        amount = districts["metric"]["amount"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"district": district, "count": count,"amount":amount, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfd = pd.concat([dfd, df_temp], ignore_index=True)
                    for pincodes in pincodes:
                        pincode = pincodes["entityName"]
                        count = pincodes["metric"]["count"]
                        amount = pincodes["metric"]["amount"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"pincode": pincode, "count": count,"amount":amount, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfpc = pd.concat([dfpc, df_temp], ignore_index=True)
    return dfs,dfd,dfpc


#function for return datafame from top trans india 
def top_trans_india_state(path_top_trans_india_state):
    dic_top_trans_india_state = path_top_trans_india_state
    dfsd = pd.DataFrame()
    dfspc = pd.DataFrame()
    for state in os.listdir(dic_top_trans_india_state):
        states = state
        state_path = os.path.join(dic_top_trans_india_state, state)
        # print(state_path)
        for year in os.listdir(state_path):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(state_path, year)  
                # print(year_path)
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_mts = json.load(json_file)
                    # states = state
                    districts = data_mts["data"]["districts"]
                    pincodes = data_mts["data"]["pincodes"]
                    # print(hoverData)
                    for districts in districts:
                        district = districts["entityName"]
                        count = districts["metric"]["count"]
                        amount = districts["metric"]["amount"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state":states,"district": district, "count": count,"amount":amount, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfsd = pd.concat([dfsd, df_temp], ignore_index=True)
                    for pincodes in pincodes:
                        pincode = pincodes["entityName"]
                        count = pincodes["metric"]["count"]
                        amount = pincodes["metric"]["amount"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        if pincode :
                            row = {"state":states,"pincode": pincode, "count": count,"amount":amount, "qyear": qyear,"quater": quater}
                            res_list = []
                            res_list.append(row)
                            df_temp = pd.DataFrame(res_list)
                            dfspc = pd.concat([dfspc, df_temp], ignore_index=True)
    return dfsd,dfspc



#function for return datafame from top user india 
def top_user_india(path_top_user_india):
        dic_top_user_india = path_top_user_india
        dfs = pd.DataFrame()
        dfd = pd.DataFrame()
        dfpc = pd.DataFrame()
        for year in os.listdir(dic_top_user_india):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(dic_top_user_india, year)
                # print(year_path) 
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_mt = json.load(json_file)   
                    states = data_mt["data"]["states"]
                    districts = data_mt["data"]["districts"]
                    pincodes = data_mt["data"]["pincodes"]
                    # print(hoverData)
                    for states in states:
                        # print(state)
                        state = states["name"]
                        registeredUsers = states["registeredUsers"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state": state, "registeredUsers": registeredUsers, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfs = pd.concat([dfs, df_temp], ignore_index=True)
                    for districts in districts:
                        district = districts["name"]
                        registeredUsers = districts["registeredUsers"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"district": district, "registeredUsers": registeredUsers, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfd = pd.concat([dfd, df_temp], ignore_index=True)
                    for pincodes in pincodes:
                        pincode = pincodes["name"]
                        registeredUsers = pincodes["registeredUsers"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"pincode": pincode,  "registeredUsers": registeredUsers, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfpc = pd.concat([dfpc, df_temp], ignore_index=True)
        return dfs,dfd,dfpc




#function for return datafame from top user state
def top_user_india_state(path_top_user_india_state):
    dic_top_user_india_state = path_top_user_india_state
    dfsd = pd.DataFrame()
    dfspc = pd.DataFrame()
    for state in os.listdir(dic_top_user_india_state):
        states = state
        state_path = os.path.join(dic_top_user_india_state, state)
        # print(state_path)
        for year in os.listdir(state_path):
            if year in ["2018", "2019", "2020", "2021","2022"]:
                year_path = os.path.join(state_path, year)  
                # print(year_path)
                for q in os.listdir(year_path):
                    year_file = os.path.join(year_path, q)
                    # print(year_file)
                    with open(year_file) as json_file:
                        data_mts = json.load(json_file)
                    # states = state
                    districts = data_mts["data"]["districts"]
                    pincodes = data_mts["data"]["pincodes"]
                    # print(hoverData)
                    for districts in districts:
                        district = districts["name"]
                        registeredUsers = districts["registeredUsers"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state":states,"district": district, "registeredUsers":registeredUsers, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfsd = pd.concat([dfsd, df_temp], ignore_index=True)
                    for pincodes in pincodes:
                        pincode = pincodes["name"]
                        registeredUsers = pincodes["registeredUsers"]
                        qyear = int(year)
                        if q == "1.json":
                            quater = "Q1"
                        elif q == "2.json":
                            quater = "Q2"
                        elif q == "3.json":
                            quater = "Q3"
                        elif q == "4.json":
                            quater = "Q4"   
                        row = {"state":states,"pincode": pincode, "registeredUsers":registeredUsers, "qyear": qyear,"quater": quater}
                        res_list = []
                        res_list.append(row)
                        df_temp = pd.DataFrame(res_list)
                        dfspc = pd.concat([dfspc, df_temp], ignore_index=True)
    return dfsd,dfspc


if __name__ == "__main__":
    dic=r'pulse\data\aggregated\transaction\country\india'
    dic_state=r'pulse\data\aggregated\transaction\country\india\state'
    dic_user_india=r'pulse\data\aggregated\user\country\india'
    dic_user_india_state = r"pulse\data\aggregated\user\country\india\state"
    dic_map_trans_india = r'pulse\data\map\transaction\hover\country\india'
    dic_map_trans_india_state = r'pulse\data\map\transaction\hover\country\india\state'
    dic_map_user_india  = r'pulse\data\map\user\hover\country\india'
    dic_map_user_india_state = r"pulse\data\map\user\hover\country\india\state"
    dic_top_trans_india = r'pulse\data\top\transaction\country\india'
    dic_top_trans_india_state = r'pulse\data\top\transaction\country\india\state'
    dic_top_user_india = r'pulse\data\top\user\country\india'
    dic_top_user_india_state = r'pulse\data\top\user\country\india\state'

    # print(agg_trans_india(dic))
    # print(agg_trans_india_state(dic_state))
    # print(agg_user_india(dic_user_india))
    #print(agg_user_india_state(dic_user_india_state))
    # print(map_trans_india(dic_map_trans_india))
    print(map_trans_india_states(dic_map_trans_india_state))
    # print(map_user_india(dic_map_user_india))
    #print(map_user_india_states(dic_map_user_india_state))
    # dfs,dfd,dfpc = top_trans_india(dic_top_trans_india)
    # print(dfs)
    # print(dfd)
    # print(dfpc)
    # dfsd,dfspc = top_trans_india_state(dic_top_trans_india_state)
    # print(dfsd)
    # print(dfspc)
    # dfs,dfd,dfpc = top_user_india(dic_top_user_india)
    # print(dfs)
    # print(dfd)
    # print(dfpc)
    # dfsd,dfspc = top_user_india_state(dic_top_user_india_state)
    # print(dfsd)
    # print(dfspc)




# to convert to csv file 

# agg_trans_india(dic).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\agg_trans_india.csv')
# agg_trans_india_state(dic_state).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\agg_trans_india_state.csv')
# agg_user_india(dic_user_india).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\agg_user_india.csv')
# agg_user_india_state(dic_user_india_state).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\agg_user_india_state.csv')
# map_trans_india(dic_map_trans_india).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\map_trans_india.csv')
# map_trans_india_states(dic_map_trans_india_state).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\map_trans_india_states.csv')
# map_user_india(dic_map_user_india).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\map_user_india.csv')
# map_user_india_states(dic_map_user_india_state).to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\map_user_india_states.csv')
# dfs,dfd,dfpc = top_trans_india(dic_top_trans_india)
# dfs.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_trans_india_dfs.csv')
# dfd.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_trans_india_dfd.csv')
# dfpc.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_trans_india_dfpc.csv')
# dfsd1,dfspc1 = top_trans_india_state(dic_top_trans_india_state)
# dfsd1.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_trans_india_state_dfsd.csv')
# dfspc1.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_trans_india_state_dfspc.csv')
# dfs2,dfd2,dfpc2 = top_user_india(dic_top_user_india)
# dfs2.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_user_india_dfs.csv')
# dfd2.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_user_india_dfd.csv')
# dfpc2.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_user_india_dfpc2.csv')
# dfsd3,dfspc3 = top_user_india_state(dic_top_user_india_state)
# dfsd3.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_user_india_state_dfsd3.csv')
# dfspc3.to_csv(r'C:\Users\GOUTHEMA RAJA\Desktop\guvi\projects\Phonepe Pulse\csv files\top_user_india_state_dfspc3.csv')
