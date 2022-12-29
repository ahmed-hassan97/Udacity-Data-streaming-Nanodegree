import faust

app = faust.App(
    'hassans' , 
    broker='localhost:9092',
    value_serializer='raw')

greetings_topic = app.topic('hassans')

@app.agent(greetings_topic)
async def greet(greetings):
    """Example agent."""
    async for greeting in greetings:
        print(f'MYAGENT RECEIVED -- {greeting}')
    

if __name__ == '__main__':
    app.main()
##faust -A app worker
