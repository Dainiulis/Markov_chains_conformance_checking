import numpy as np
from scipy.optimize import curve_fit


class LogarithmicRegression():

    def __init__(self, x, y):
        """x ir y ašis.
         Leidžiami tipai - numpy array arba list"""
        if isinstance(x, list):
            x = np.array(x)
        if isinstance(y, list):
            y = np.array(y)
        self.popt, self.pcov = curve_fit(self._function, x, y)

    def _function(self, x, a, b):
        """Logaritminė funkcija"""
        return a + b * np.log(x)

    def predict(self, x):
        """grąžina numanomą reikšmę
        x gali būti vienas skaičius arba list arba numpy array"""
        if isinstance(x, list):
            x = np.array(x)
        return self._function(x, *self.popt)