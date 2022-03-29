import joblib
import pandas as pd

class Transformer(object):
    """
    this class loads the encoder filesets and apply it to the data passed
    """
    def __init__(self):
        self.encoder = joblib.load('FlgithsDelayOrdinalEncoder.pkl')

    def transform_input(self, X, feature_names, meta):
        '''
            Seldon will call this function to apply the transformation
        '''    
        df = pd.DataFrame(X, columns=feature_names)
        df = self.encoder.transform(df)
        return df.to_numpy()