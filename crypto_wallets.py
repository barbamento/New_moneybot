import numpy as np
import pandas as pd
import coinmarketcapapi
import json
import datetime
from binance import client
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

class nexo_wallet:
    def __init__(self):
        self.path=os.path.join("DB","nexo")
        self.inizialize_database()
        nexo_plan=self.calculate_nexo_plan()
    
    def inizialize_database(self):
        '''
        load all the csv files.
        terms has all the apy varying between plans and asset
        stacked  
        '''
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        '''
        load/creates the database with the earn terms
        '''
        terms_path=os.path.join(self.path,"nexo_earn_terms.csv")
        if not os.path.exists(terms_path):
            df=pd.DataFrame(create_plat_nexo_terms(),columns=["asset","nexo_term","apy"])
            df.to_csv(terms_path)
        '''
        load/creates the databaes with the stacked money
        '''
        terms_path=os.path.join(self.path,"stacked.csv")
        if not os.path.exists(terms_path):
            df=pd.DataFrame(columns=["asset","ammount","date_in","date_out","bonus","interest_accrued"])
            df.to_csv(terms_path)
        '''
        load/creates the databaes with the free money
        '''
        free_path=os.path.join(self.path,"free.csv")
        if not os.path.exists(free_path):
            df=pd.DataFrame(columns=["asset","ammount"])
            df.to_csv(free_path)
        '''
        load/creates the databaes with the movements
        '''
        movements_path=os.path.join(self.path,"movements.csv")
        if not os.path.exists(movements_path):
            df=pd.DataFrame(columns=["asset","date","ammount","reason","cost"])
            df.to_csv(movements_path)
        



    def calculate_nexo_plan(self):
        return "platinum"

    

def create_plat_nexo_terms():
    rows=[
        ["USDT","platinum",10],
        ["USDC","platinum",10],
        ["USDP","platinum",10],
        ["UST","platinum",15],
        ["GBPX","platinum",10],
        ["DAI","platinum",10],
        ["EURX","platinum",10],
        ["TUSD","platinum",10],
        ["USDX","platinum",10],
        ["USDT","platinum",10],
        ["BTC","platinum",6],
        ["ETH","platinum",6],
        ["NEXO","platinum",12],
        ["AXS","platinum",34],
        ["MATIC","platinum",14],
        ["KSM","platinum",10],
        ["DOT","platinum",13],
        ["APE","platinum",10],
        ["AVAX","platinum",10],
        ["NEAR","platinum",8],
        ["BNB","platinum",6],
        ["ADA","platinum",6],
        ["SOL","platinum",6],
        ["ATOM","platinum",9],
        ["FTM","platinum",8],
        ["LUNA","platinum",6],
        ["XRP","platinum",6],
        ["LTC","platinum",6],
        ["LINK","platinum",6],
        ["BCH","platinum",6],
        ["TRX","platinum",6],
        ["XLM","platinum",6],
        ["EOS","platinum",6],
        ["PAXG","platinum",6],
        ["DOGE","platinum",1]
    ]
    return rows

if __name__=="__main__":
    nexo_wallet()