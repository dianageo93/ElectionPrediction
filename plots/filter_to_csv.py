import sys
import datetime
import os
from collections import defaultdict

key_words = ['clinton', 'trump', 'democrat', 'republican']

def build_file_list(start_time, end_time):
    file_list = []
    while start_time <= end_time:
        file_list.append(start_time)
        start_time += datetime.timedelta(hours=1)
    return file_list

def process_files(data_dir, file_list):
    wiki_topics = defaultdict(list)
    start_time = file_list[0]
    for file_name in file_list:
        path = os.path.join(data_dir, 'pageviews-' + file_name.strftime('%Y%m%d-%H%M%S'))
        with open(path) as f:
            lines = f.readlines()
        today = defaultdict(int)
        for line in lines:
            try:
                splits = line.split(' ')
                for key_word in key_words:
                    if key_word in splits[1].lower():
                        today[splits[1]] += int(splits[2])
                        break
            except:
                pass

        for page, views in today.iteritems():
            wiki_topics[page].append((file_name, views))


    matrix = [[0 for col in xrange(len(wiki_topics))] for row in xrange(len(file_list))]
    col = 0
    header = ['Date']
    for k, v in wiki_topics.iteritems():
        header.append(k)
        for time, views in v:
            row = (time - start_time).seconds / 3600
            matrix[row][col] = views
        col += 1

    print ' '.join(header)
    for row in xrange(len(matrix)):
        date = 'Date({}) '.format(file_list[row].strftime('%Y,%m,%d,%H,%M,%S'))
        print date, ' '.join(map(str, matrix[row]))

def __main__():
    if len(sys.argv) < 4:
        print 'Usage: ./filter_to_csv.py data_dir from_time to_time'
        return
    data_dir = sys.argv[1]
    start_time = datetime.datetime.strptime(sys.argv[2], '%Y%m%d-%H%M%S')
    end_time = datetime.datetime.strptime(sys.argv[3], '%Y%m%d-%H%M%S')
    file_list = build_file_list(start_time, end_time)

    process_files(data_dir, file_list)

if __name__ == '__main__':
    __main__()
