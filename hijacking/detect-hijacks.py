import pybgpstream
import pprint
import numpy as np
import itertools

def stringify(list):
	if len(list) == 1:
		return list[0]
	sep2 = " and "
	sep1 = ", "
	return sep2.join([sep1.join(list[:-1]), list[-1]])

# Defining collector to detect the Prefix Hijacking.
collector = "route-views2"
print("Collector: " + collector)

# Defining timelapses to detect the Prefix Hijacking.
timelapse1 = {
	"init": "2019-02-26 23:58:00",
	"end": "2019-02-26 23:59:00"
}
timelapse2 = {
	"init": "2019-02-26 23:59:00",
	"end": "2019-02-27 00:00:00"
}
timelapse3 = {
	"init": "2019-02-27 00:00:00",
	"end": "2019-02-27 00:01:00"
}
timelapse4 = {
	"init": "2019-02-27 00:01:00",
	"end": "2019-02-27 00:02:00"
}
timelapse5 = {
	"init": "2019-02-27 00:02:00",
	"end": "2019-02-27 00:03:00"
}
timelapses = [timelapse1, timelapse2, timelapse3, timelapse4, timelapse5]
print("Timelapses: " + timelapses[0]["init"] + " to " + timelapses[-1]["end"])

stream_data = []
for idx, timelapse in enumerate(timelapses):

	# Requesting API
	print("Requesting information from BPGStream @ Timelapse " + str(idx) + ".")
	stream = pybgpstream.BGPStream(
		from_time=timelapse["init"],
		until_time=timelapse["end"],
		filter="type ribs and collector %s" % (collector)
	)

	# Getting pairs prefix/AS.
	print("Parsing data @ Timelapse " + str(idx) + ".")
	raw_data = []
	for elem in stream:
		raw_data += [{
			"prefix": elem.fields["prefix"],
			"as-announcer": elem.fields["as-path"].split(' ')[-1]
		}]

	# Grouping by prefix
	print("Grouping entries by prefixes @ Timelapse " + str(idx) + ".")
	groups = itertools.groupby(raw_data, key=lambda element: element["prefix"])
	
	# Removing duplicated AS and adding to Stream Data.
	print("Removing duplicated ASes @ Timelapse " + str(idx) + ".")
	for key, value in groups:
		stream_data += [{
			"prefix": key,
			"timelapse": idx,
			"as-announcers": list(np.unique(np.array((map(lambda elem: elem["as-announcer"], value)))))
		}]

# Grouping by prefix
print("Grouping entries by prefixes through every timelapse.")
prefixes = {}
for elem in stream_data:
	if elem["prefix"] in prefixes.keys():
		prefixes[elem["prefix"]] += [{"ases": elem["as-announcers"], "timelapse": elem["timelapse"]}]
	else:
		prefixes[elem["prefix"]] = [{"ases": elem["as-announcers"], "timelapse": elem["timelapse"]}]

# Evaluating prefixes announced by differents ASes in different timelapses.
print("Comparing results across every timelapse.")
for prefix, ases_by_timelapse in prefixes.items():
	ases = list(map(lambda elem: elem["ases"], ases_by_timelapse))

	same_ases = True
	as_comparator = ases[0]
	for as_ in ases:
		same_ases = same_ases and (as_.sort() == as_comparator.sort())
		as_comparator = as_

	if not same_ases:
		print("Possible prefix hijacking, " + prefix + " announced by: ")

		for as_by_timelapse in ases_by_timelapse:
			print("ASes " + stringify(as_by_timelapse["ases"]) + " in timelapse " + str(as_by_timelapse["timelapse"]))
