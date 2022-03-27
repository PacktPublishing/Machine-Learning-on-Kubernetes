
import joblib


class Predictor(object):

    def __init__(self):
        self.model = joblib.load('model.pkl')

    def predict(self, data_array, column_names):
        return self.model.predict_proba(data_array)
