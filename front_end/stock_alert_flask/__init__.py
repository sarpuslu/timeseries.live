from flask import Flask
app = Flask(__name__)
from stock_alert_flask import views
