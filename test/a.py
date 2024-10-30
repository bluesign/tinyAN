import json
import requests
from trie import HexaryTrie
import rlp



def makeRequest(p, url="http://localhost/"):
    headers = {"Content-Type": "application/json"}
    response = requests.request("POST", url, headers=headers, data=json.dumps(p))
    return response.json()


def decodeHash(s):
    s = s.replace("0x","")
    s = bytes.fromhex(s)
    while len(s)<32:
        s=b"\x00"+s
    return s

#verify_block_tx_root(1020679, "https://cloudflare-eth.com/")

def checkBlock(height, block):
    assert(block["number"] == hex(height))
    receiptsRootHash = decodeHash(block["receiptsRoot"])
    rootHash = decodeHash(block["transactionsRoot"])
    trie = HexaryTrie(db={})
    c=0
    txs = []
    for t in block["transactions"]:
        assert(t["blockHash"]==block["hash"])
        assert(t["blockNumber"]==hex(height))
        trie[rlp.encode(c)] = decodeHash(t["hash"])
        txs.append(t["hash"])
        c=c+1
    assert(rootHash.hex()==trie.root_hash.hex())
    return txs


def trace(startHeight, endHeight):
    p=[]
    for height in range(startHeight, endHeight):
        p.append( {
                "method": "debug_traceBlockByNumber",
                "params": [hex(height)], 
                "id": height,
                "jsonrpc": "2.0"
            })

    #trace block
    return makeRequest(p)

def checkHeight(startHeight, endHeight):
    traceResults = trace(startHeight, endHeight)
    p=[]
    for height in range(startHeight, endHeight):
        p.append( {
                "method": "eth_getBlockByNumber",
                "params": [hex(height), True], 
                "id": height,
                "jsonrpc": "2.0"
            })
    
    
    result = makeRequest(p)
    
    index = 0
    for r in result:
        block = r["result"]
        txs = checkBlock(r["id"], block)
        traceResult = traceResults[index]["result"]
        assert(len(txs)==len(traceResult))    
        c=0
        for t in txs:
            tt = traceResult[c]["txHash"]
            r =  traceResult[c]["result"]
            c=c+1
            assert(tt==t)
            assert(len(json.dumps(r))>10)

        index=index+1
    
    print(startHeight)




for i in range(0, 5000000, 1000):
   checkHeight(i, i+1000)

