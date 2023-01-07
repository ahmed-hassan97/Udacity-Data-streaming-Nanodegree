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

app = faust.App(
    'hassans' , 
    broker='localhost:9092'
    )


user_info_topic = app.topic("info" , value_type=user_info)
currency_summary_table = app.Table("currency_summary" , default=int)

@app.agent(user_info_topic)
async def greet(users_info):
    """Example agent."""
    #.filter(lambda x: x.currency == "AUD")
    async for data in users_info.group_by(user_info.currency):
            currency_summary_table[data.currency] += data.amount
            print(f"{data.currency} : {currency_summary_table[data.currency]}")


        
        
      
       
       
    

if __name__ == '__main__':
    app.main()
##faust -A app worker
## sudo kill -9 $(ps -ef | grep Faust | awk '{print $2}')
