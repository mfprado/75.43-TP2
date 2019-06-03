import pybgpstream
import pprint
import numpy as np
from itertools import groupby
import pandas as pd

COLLECTOR='route-views.saopaulo'
TIME_INIT="2015-06-06"
TIME_END="2015-06-06 00:01"


stream = pybgpstream.BGPStream (
from_time = TIME_INIT ,
until_time = TIME_END ,
filter = "type  ribs  and  collector  %s" % (COLLECTOR))

write_file = "rib_ej2.csv"
with open(write_file, "w") as output:
    for element in stream:
        output.write(element.fields['as-path'].split(' ')[-1] + '     '+ element.fields['prefix'] + '\n')
