import multiprocessing as mp
import operator
FILENAME = 'simple.txt'
NUM_WORKERS = 8

def run_parallel_map(func, N, data) -> list:
    processes = []
    for i in range(NUM_WORKERS):
        start = (int) (N / NUM_WORKERS * i)
        stop = (int) (N / NUM_WORKERS * (i+1))

        p = mp.Process(target=func, args=(q, data, start, stop))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    data = []
    while not q.empty():
        data.extend(q.get())

    return data

def textFile(name) -> list:
    with open('./' + name) as f:
        return [w.strip() for w in f.readlines()]

def flatMap(q, lines, start, stop):
    words = []
    for i in range(start, stop):
        words.extend(lines[i].split(" "))

    q.put(words)

def reduce_add(q, pair):
    vals = pair[1]
    result = sum(vals)

    q.put( (pair[0], result) )

def map_cnt(q, words, start, stop):
    pairs = []
    for i in range(start, stop):
        pairs.append( (words[i], 1) )

    q.put(pairs)

if __name__ == '__main__':
    q = mp.Queue() # atomic data structure
    
    # Read file
    lines = textFile(FILENAME)

    # Perform flat map
    N = len(lines)
    words = run_parallel_map(flatMap, N, lines)

    # Map word -> (word, count)
    N = len(words)
    pairs = run_parallel_map(map_cnt, N, words)

    # Reduce, start a process for each (k, [v])
    reduce_in = {}
    for pair in pairs:
        key = pair[0]
        val = pair[1]
        if key in reduce_in:
            reduce_in[key].append(val)
        else:
            reduce_in[key] = [val]


    processes = []
    for key in reduce_in:
        p = mp.Process(target=reduce_add, args=(q, (key, reduce_in[key])))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    counts = []
    while not q.empty():
        counts.append(q.get())

    # print(counts)

    getcount = operator.itemgetter(1)
    sorted_counts = dict(sorted(counts, key=getcount, reverse=True))
    print(sorted)