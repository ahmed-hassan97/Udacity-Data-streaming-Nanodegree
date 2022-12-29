import faust
from dataclasses import dataclass, field
import json 

@dataclass
class user_info(faust.Record):
    username : str 
    currency: str 
    address: str 
    amount: int 
    id: int

@dataclass
class fraud_user_info(faust.Record):
    username : str 
    currency: str 
    amount: int 
    fraud: str="fraud"

@dataclass
class not_fraud_user_info(faust.Record):
    username : str 
    currency: str 
    amount: int 
    fraud: str="notfraud"   
   

app = faust.App(
    'hassans' , 
    broker='localhost:9092'
    )

user_info_topic = app.topic("info" , value_type=user_info)
fraud_info_topic = app.topic(
            "fraud_user_info" , 
            key_type=str , 
            value_type=fraud_user_info
            )
not_fraud_info_topic = app.topic(
            "not_fraud_user_info", 
            key_type=str , 
            value_type=not_fraud_user_info
            )
@app.agent(user_info_topic)
async def greet(users_info):
    """Example agent."""
    #.filter(lambda x: x.currency == "AUD")
    async for data in users_info.filter(lambda x: x.currency == "AUD" and x.amount >= 10000):
        if data.currency == "USD" or data.amount > 60000:
            filtered_data = fraud_user_info(
             username = data.username,
             currency = data.currency,
             amount = data.amount
                )
            await fraud_info_topic.send(key=filtered_data.username , value = filtered_data)    
        else:
            filtered_data = not_fraud_user_info(
             username = data.username,
             currency = data.currency,
             amount = data.amount
            )
            await not_fraud_info_topic.send(key=filtered_data.username , value = filtered_data)


        
        
      
       
       
    

if __name__ == '__main__':
    app.main()
##faust -A app worker
## sudo kill -9 $(ps -ef | grep Faust | awk '{print $2}')
