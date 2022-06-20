import boto3
import pandas as pd
import json
from sqlalchemy import create_engine
from datetime import datetime
import sys
import os

s3 = boto3.resource('s3')
bucket_name = "tiridico-site-2022"


def dataConcatenate(*args,concatType = "row"):
    concatType = concatType.lower()
    if concatType == "row": 
        return pd.concat(*args)
    elif concatType == "column":
        return pd.concat(*args, axis = 1)
    
    
    
    
def ListFolder(csvFilePath:str=None, Prefix:str=None) -> pd.DataFrame:
    tempList = []
    bucket = s3.Bucket(bucket_name)
    if not Prefix:
        for obj in bucket.objects.all():
            tempList.append(obj.key) 
        if csvFilePath != None:
            tempDf = pd.DataFrame(tempList, columns=["FileName"])
            tempDf.to_csv(csvFilePath)
            return tempDf   
    else:
        for obj in bucket.objects.filter(Prefix=Prefix):
            tempList.append(obj.key.lstrip(Prefix)) 
        if csvFilePath != None:
            
            tempDf = pd.DataFrame(tempList, columns=["FileName"])
            tempDf.to_csv(csvFilePath)
            return tempDf
    return pd.DataFrame(tempList, columns = ["FileName"])



def searchItem(userId:int, offerId:int, offerItemId:int):
    prefix = "offer-files/" + str(userId) + "/" + str(offerId) + "/" + str(offerItemId) + "/"
    return ListFolder(Prefix = prefix)



# %%


def downloadData(start_year:int = 2022, start_month: int = 5, start_day:int = 23):
    CONFIG = json.load(open('config.json'))
    start_time = str(datetime(start_year,start_month,start_day))
    query = """
            select *
                from public."OfferItem"
                full outer join public."Offer"
                on public."Offer"."OfferId" = public."OfferItem"."OfferId"
                where public."Offer"."RequestType" = 1 
                and public."Offer"."CreateDate" > '"""+ start_time +"""'
                and public."OfferItem"."OfferItemId" is not null
                and public."OfferItem"."Price" != 0
                order by public."OfferItem"."CreateDate" desc
            """
    DB_KEY = CONFIG["DB_KEY"]
    ENGINE_URL = 'postgresql://'+DB_KEY["User"]+':'+DB_KEY["Pswd"]+'@'+DB_KEY["awsKey"]+'/'+DB_KEY["DbName"]
    dbEngine = create_engine(ENGINE_URL)
    data = pd.read_sql_query(query, dbEngine)
    DataIds = dataConcatenate([pd.DataFrame(data.iloc[:,1]),
                             pd.DataFrame(data.iloc[:,20]),
                             pd.DataFrame(data.iloc[:,15]),
                             pd.DataFrame(data.iloc[:,0])],
                             concatType = "column")
    return data, DataIds
# %%
def createtargetFile(target_name = "CadFiles"):
    root_list = os.listdir()
    target_root = target_name + "/"
    if not target_root.split("/")[0] in root_list:
        os.mkdir(target_root.split("/")[0])









# %%




def main():
    
    
    data, DataIds = downloadData(2022,5,23)    
    createtargetFile()
    
    
    listRepo = []
    for i in range(DataIds.shape[0]):
        prefix = "offer-files/" + str(DataIds.UserId[i]) + "/" + str(DataIds.OfferId[i]) + "/" + str(DataIds.OfferItemId[i])
        fileList = ListFolder(Prefix = prefix)
        fileList = list(fileList.FileName)
        listRepo.append(fileList) 
        x = int(i / DataIds.shape[0] * 100)
        print(f'\r Cad Files are downloading..: % {x}', end='', flush=True)


        
        for j in range(len(fileList)):
            if "cad-file" in fileList[j]:
                file_root = "offer-files/" + str(DataIds.UserId[i]) + "/" + str(DataIds.OfferId[i]) + "/" + str(DataIds.OfferItemId[i]) + "/" + str(fileList[j])
                s3.Bucket(bucket_name).download_file(file_root, "CadFiles/" + fileList[j].split("/")[1]) 
                
    print("\n İndirme Tamamlandı")






if __name__=="__main__":
    main()
