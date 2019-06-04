import pybgpstream
import numpy as np
import itertools

def stringify(list):
	sep1 = ", "
	sep2 = " and "
	return sep2.join([sep1.join(list[:-1]), list[-1]])

# Timelapse to detect the prefix hicjackign.
TIME_INIT = "2019-02-27 00:00:00"
TIME_END = "2019-02-27 00:01:00"

# Collector used to detect it.
COLLECTOR = 'route-views2'
print("Collector: " + COLLECTOR)

# Requesting API
print("Requesting information from BPGStream.")
stream = pybgpstream.BGPStream(
	from_time=TIME_INIT,
	until_time=TIME_END,
	filter="type ribs and collector %s" % (COLLECTOR)
)

# Getting pairs prefix/AS.
print("Parsing data.")
raw_data = []
for elem in stream:
	raw_data += [{
		"prefix": elem.fields["prefix"],
		"as-announcer": elem.fields["as-path"].split(' ')[-1]
	}]

# Grouping entries by prefixes.
print("Grouping entries by prefixes.")
groups = itertools.groupby(raw_data, key=lambda element: element["prefix"])
	
# Removing duplicated ASes.
print("Removing duplicated ASes.")
prefixes = {}
for key, value in groups:
	prefixes[key] = list(np.unique(np.array((map(lambda elem: elem["as-announcer"], value)))))
	
# Evaluating prefixes announced by more than one AS.
print("Evaluating prefixes announced by more than one AS.")
for prefix, as_announcer in prefixes.items():
	if (len(as_announcer) > 1):
		print("Possible Prefix Hijacking: " + prefix + " announced by " + stringify(as_announcer) + ".")