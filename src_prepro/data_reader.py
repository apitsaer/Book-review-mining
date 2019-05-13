import pandas as pd
import os
import json

SOURCE_DIR = "../../data_all"
DEST_FILE = "../data_processed/ExctractedData.json"
output_file = open(DEST_FILE, 'w')
data = []
review_id_set = set()

print("Parsing files to " + DEST_FILE)

count_files = 0
count_unique = 0
# loop through the folder structure
for currentFolder in os.listdir(SOURCE_DIR):
    # only consider relevant folders
    if str(currentFolder).startswith('data'):
        for currentFile in os.listdir(SOURCE_DIR + "/" + currentFolder):
            # check if file contains review
            if str(currentFile).startswith('part'):
                file = SOURCE_DIR + "/" + currentFolder + "/" + currentFile
                # open the file and save parse the lines (possiblye more than 1) in the JSON file
                with open(file, 'r') as f:
                    for line in f:
                        jsonData = json.loads(line)
                        data.append(jsonData)
                        count_files += 1
                        if not jsonData["review_id"] in review_id_set:
                            review_id_set.add(jsonData["review_id"])
                            count_unique += 1
                # show progress
                if(count_files % 100 ==0):
                    print("\t @ review: " + str(count_files))

# show some stats            
#nReview = df['review_id'].nunique()
print("# rows = {} \t # unique reviews = {}".format(count_files, count_unique))

# save file
json.dump(data, output_file)
output_file.close()
print("File saved in " +  DEST_FILE)

