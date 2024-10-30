import requests
import json

def makeRequest(method, params, url="http://tiny.dnz.dev/"):
    p=[]
    for item in params:
        p.append( {
            "method": method,
            "params": [hex(item)],
            "id": item,
            "jsonrpc": "2.0"
        })
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=json.dumps(p))
    return response.json()


# 4595104 - 4594890

import queue
import threading
import time


# The queue for tasks
q = queue.Queue()


# Worker, handles each task
def worker():
    while True:
        items = []
        for i in range(1):
            item = q.get()
            if item is None:
                break
            items.append(item)

        print("Working on", items[0])
        r = makeRequest("debug_traceBlockByNumber", items)
        print(item)
        q.task_done()


def start_workers(worker_pool=1000):
    threads = []
    for i in range(worker_pool):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    return threads


def stop_workers(threads):
    # stop workers
    for i in threads:
        q.put(None)
    for t in threads:
        t.join()


def create_queue(task_items):
    for item in task_items:
        q.put(item)

from typing import List
import hashlib

class Node:
    def __init__(self, left, right, value: bytes, content) -> None:
        self.left = left
        self.right = right
        self.value = value
        self.content = content

    @staticmethod
    def hash(val: bytes) -> bytes:
        return hashlib.sha3_256(val).digest()

    def __str__(self):
        return hex(self.value)

class MerkleTree:
    def __init__(self, values: List[bytes]) -> None:
        self.__buildTree(values)

    def __buildTree(self, values: List[bytes]) -> None:
        leaves = [Node(None, None, Node.hash(e), e) for e in values]
        while len(leaves) % 2 == 1:
            leaves.append(leaves[-1])  # Duplicate last element if odd number of elements
        self.root = self.__buildTreeRec(leaves)

    def __buildTreeRec(self, nodes: List[Node]) -> Node:
        if len(nodes) == 1:
            return nodes[0]
        new_level = []
        for i in range(0, len(nodes), 2):
            left = nodes[i]
            right = nodes[i + 1]
            value = Node.hash(left.value + right.value)
            content = left.content + right.content
            new_level.append(Node(left, right, value, content))
        return self.__buildTreeRec(new_level)



    def getRootHash(self) -> bytes:
        return self.root.value


if __name__ == "__main__":

    #print(makeRequest("debug_traceBlockByNumber", [2,3]))

    #deniz

    # Dummy tasks
    tasks = [item for item in range(718, 719)]

    for task in tasks:
        j = makeRequest("debug_traceBlockByNumber", [task])
        result = j[0]["result"]
        txHashes = []
        for r in result:
            txHashes.append(bytes.fromhex(r["txHash"].replace("0x", "")))

                
        if len(txHashes)>0:
            mtree = MerkleTree(txHashes)
            print("Root Hash: " , mtree.getRootHash().hex(), "\n")
            print(task, txHashes)   



    # Start up your workers
    #workers = start_workers(worker_pool=10)
    #create_queue(tasks)

    # Blocks until all tasks are complete
    #q.join()

    #stop_workers(workers)

