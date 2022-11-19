import pandas as pd
from datetime import datetime


data = []
for i in range(2,1000000,1):
    id = i
    name = "te{}gwr{}@jd66e@{}".format(i , i+100 , i-100)
    email = "ah{}hdg123P{}@gmail.com".format(i  , i-100)
    password = "pj{}swq{}@mn7@{}".format(i-500,i+24  , i-100)
    amount = id * 8
    date  = datetime.now()
    
    data.append([id ,name, email , password , amount , date ])

    
structured_data = pd.DataFrame(data , columns=['id','name','email','password','amount','date'])
structured_data.to_csv('data.csv' , index=False)
print(structured_data)


