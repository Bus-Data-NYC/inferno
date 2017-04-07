#!/user/bin/env python
import sys
from vincenty import vincenty

'''Calculate distance between two lng, lat points in feet'''

def main(lng1, lat1, lng2, lat2):
    print (lat1, lng1), (lat2, lng2)
    miles = vincenty((lat1, lng1), (lat2, lng2), miles=True)
    return miles * 5280.


if __name__ == '__main__':
    print main(*[float(x) for x in sys.argv[1:]])
