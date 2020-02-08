# Eggplant

## Usage
```python
app = Eggplant(RabbitKombuBroker(amqp_uri='amqp://localhost', exchange='eggplant-exchange', queue='users_service_queue'))

# Function handlers
@app.handler('user_logged_in')
def handle_user_login(message):
    User.updateLastLoginTime()

# Class handlers
@app.handler('user_logged_in')
class UserLoginHandler:
    def handle(self, message):
        User.updateLastLoginTime()
```