import multiprocessing as mp
import heapq
from heapq import heappop, heappush
import time

FILENAME = 'pride_prej.txt'
NUM_WORKERS = 8

def run_parallel_map(func, N, data) -> list:
    processes = []
    for i in range(NUM_WORKERS):
        start = (int) (N / NUM_WORKERS * i)
        stop = (int) (N / NUM_WORKERS * (i+1))

        p = mp.Process(target=func, args=(q, data, start, stop), daemon=True)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    data = []
    while not q.empty():
        data.extend(q.get())

    return data

def textFile(name) -> list:
    with open('./' + name, encoding='utf-8-sig') as f:
        return [w.strip() for w in f.readlines()]

def flatMap(q, lines, start, stop):
    words = []
    for i in range(start, stop):
        words.extend(lines[i].split(" "))

    q.put(words)

def reduce_add(q, pair):
    vals = pair[1]
    result = len(vals)

    q.put( (pair[0], result) )

def map_cnt(q, words, start, stop):
    pairs = []
    for i in range(start, stop):
        pairs.append( (words[i], 1) )

    q.put(pairs)

if __name__ == '__main__':
    q = mp.Manager().Queue() # atomic data structure
    start = time.time()

    # Read file
    lines = textFile(FILENAME)

    # Perform flat map
    N = len(lines)
    words = run_parallel_map(flatMap, N, lines)

    # Map word -> (word, count)
    N = len(words)
    pairs = run_parallel_map(map_cnt, N, words)

    # Reduce (word,count), start a process for each (k, [v])
    reduce_in = {}
    for pair in pairs:
        key = pair[0]
        val = pair[1]
        if key in reduce_in:
            reduce_in[key].append(val)
        else:
            reduce_in[key] = [val]
    reduce_in = list(reduce_in.items())

    processes = []
    index = 0
    while index < len(reduce_in):
        for i in range(NUM_WORKERS):
            key = reduce_in[index][0]
            val = reduce_in[index][1]

            p = mp.Process(target=reduce_add, args=(q, (key, val)), daemon=True)
            p.start()
            processes.append(p)
            index += 1
            if index >= len(reduce_in):
                break

        for p in processes:
            p.join()

    counts = []
    while not q.empty():
        counts.append(q.get())

    print(counts[:5])
    
    # Collect and print top 5 most frequent
    sorted_counts = sorted(counts, key=lambda x: x[1], reverse=True)
    r = sorted_counts[:5]
    print(r)

    # Computation time
    print(time.time() - start)