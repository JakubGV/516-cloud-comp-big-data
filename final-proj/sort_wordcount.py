import multiprocessing as mp
import operator
import heapq
from heapq import heappop, heappush
import time
FILENAME = 'simple.txt'
NUM_WORKERS = 8

# def new_cmp_lt(self,a,b):
#     return a[1][1]<b[1][1]

# #override the existing "cmp_lt" module function with your function
# heapq.cmp_lt=new_cmp_lt

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

def map_sort(q, pairs, start, stop):
    out = []
    sorted_pairs = sorted(pairs[start:stop], key=lambda x: x[1], reverse=False)
    out.append( ("list", sorted_pairs) )

    q.put(out)

# [('dog', 3), ('cat', 2)]
# [('cow', 5), ('sheep', 3)]
# [('word', 3), ('word2', 2)]
# [('word', 3), ('word2', 2)]
    
def reduce_sort(q, pair,N):
    lists = pair[1]
    # print(lists)
    pq = [(x[0][1],i,x[0]) for i,x in enumerate(lists)]
    heapq.heapify(pq)

    sorted_words = []
    # print(pq)
    pointers = [1 for i in range(N)]
    # heapq.show_tree(heap)
    while pq:
        _min = heappop(pq)

        sorted_words.append(_min[2])
        if pointers[_min[1]] < len(lists[_min[1]]):
            
            # print(lists[_min[1]][pointers[_min[1]]])
            insert = lists[_min[1]][pointers[_min[1]]]
            heappush(pq,(insert[1],_min[1],insert))
            pointers[_min[1]] += 1

    return q.put(sorted_words)
    # pass

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

    # print(pairs)
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
    start_time = time.time()
    
    #sorting with built in
    print( len(counts))
    sorted_counts = list(sorted(counts, key=operator.itemgetter(1), reverse=True))
    print(f'Execution Time: {time.time() - start_time}')
    
    # sorting using out custom map reduce
    # Sort by counts, ascending
    q = mp.Queue() # atomic data structure

    N = len(counts)
    sorted_counts = run_parallel_map(map_sort, N, counts)
    
    start_time = time.time()
    # Reduce, start a process for each (k, [v])
    reduce_in = {}
    for pair in sorted_counts:

        key = pair[0]
        val = pair[1]
        if key in reduce_in:
            reduce_in[key].append(val)
        else:
            reduce_in[key] = [val]

    processes = []
    for key in reduce_in:
        p = mp.Process(target=reduce_sort, args=(q, (key, reduce_in[key]), NUM_WORKERS))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    final_counts = []
    while not q.empty():
        final_counts.append(q.get())
    print(f'Execution Time: {time.time() - start_time}')
    print(final_counts)
