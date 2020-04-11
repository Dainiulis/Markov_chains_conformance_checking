import numpy as np
from scipy.optimize import curve_fit


class ExponentialRegression():

    def __init__(self, x, y):
        """x ir y ašis.
         Leidžiami tipai - numpy array arba list"""
        if isinstance(x, list):
            x = np.array(x)
        if isinstance(y, list):
            y = np.array(y)
        if len(x) == 1:
            x = np.array([1, 2, 3])
            y = np.array([y[0], y[0]/10, 0])
        elif len(x) == 2:
            x = np.array([1, 2, 3])
            y = np.append(y, 0)
        self.popt, self.pcov = curve_fit(self._function, x, y)

    def _function(self, x, a, b):
        """Eksponentinė nykstanti funkcija"""
        return a * np.exp(b * -x)

    def predict(self, x):
        """grąžina numanomą reikšmę
        x gali būti vienas skaičius arba list arba numpy array"""
        if isinstance(x, list):
            x = np.array(x)
        return self._function(x, *self.popt)