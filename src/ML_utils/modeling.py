# modeling.py

from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics
from utils.common_utils import read_config

ML_config = read_config('..\ML_config.yml')

def train_linear_regression(X_train, y_train):
    print("inside linear regression model")
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    return lr

def train_decision_tree(X_train, y_train):
    dt = DecisionTreeRegressor(max_depth=ML_config['DT']['max_depth'], criterion=ML_config['DT']['criterion'])
    dt.fit(X_train, y_train)
    return dt

def train_random_forest(X_train, y_train):
    rf = RandomForestRegressor(max_depth=ML_config['RF']['max_depth'], n_estimators=ML_config['RF']['n_estimators'], criterion=ML_config['RF']['criterion'])
    rf.fit(X_train, y_train)
    return rf

def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    mae = metrics.mean_absolute_error(y_test, y_pred)
    rmse = metrics.mean_squared_error(y_test, y_pred, squared=False)
    return mae, rmse
