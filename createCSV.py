import os
import csv
import json

def get_filepaths(directory):
	file_paths = []

	for root,dirs, files in os.walk(directory):
		for filename in files:
			filepath=os.path.join(root,filename)
			file_paths.append(filepath)

	return file_paths

full_file_paths = get_filepaths("/home/qiuhaoling/Desktop/project/lastfm_train")
outfile = open('/tmp/output', 'w')
for fname in full_file_paths:
		with open(fname) as infile:
			data = json.load(infile)
                        data.pop("similars")
			if len(data["tags"])!=0:
                            #json.dump(data,outfile)
                            outfile.write(json.dumps(data)+'\n')
outfile.close()
