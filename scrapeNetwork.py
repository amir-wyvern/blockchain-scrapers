from web3 import Web3
from web3.middleware import geth_poa_middleware
from time import time
import redis

TOPICS = {
    '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0',
    '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
}

r = redis.Redis(host='127.0.0.1', port='6379' ,decode_responses=True)

w3 = Web3(Web3.HTTPProvider("https://bsc-dataseed.binance.org") )
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

contractAddresses = {}

def SearchOnBlockRange(startBlock, endblock):

    for blockNumber in range(startBlock, endblock):
        print('block number : ' , blockNumber)
        blockTxs = w3.eth.get_block(blockNumber ,full_transactions=True)
        findCreateMethodInTx(blockTxs['transactions'])

def findCreateMethodInTx(txs):

    for tx in txs:

        if tx['input'].startswith('0xf305d719') and hex(int(tx['input'][10:74], 16)) in contractAddresses:

            tokenAddress = hex(int(tx['input'][10:74], 16))
            print('*'*50)
            print('>find a token : {0} | value : {1:.5}'.format(tokenAddress, tx['value']/10**18 ) )
            print('*'*50)
            r.rpush('exact' , tokenAddress)
            del contractAddresses[tokenAddress]

        if tx['input'].startswith('0x60806040') and '83d6b0a2' in tx['input'] :

            txData = w3.eth.get_transaction_receipt(tx['hash'].hex())
            topicLogs = set([log['topics'][0].hex() for log in txData['logs']])

            if topicLogs and not(topicLogs ^ TOPICS):

                contractAddresses[txData['contractAddress'].lower()] = int(time())

                print('='*50)
                print('contractAddresses : ', txData['contractAddress'])
                print('='*50)
        
        

# print('20940595, 20945595')
# SearchOnBlockRange(20987207, 20987210)

block_number = w3.eth.get_block_number()
old_block_number = block_number

while True:
    
    block_number = w3.eth.get_block_number()
    if block_number != old_block_number:
        SearchOnBlockRange(old_block_number, block_number)
        old_block_number = block_number
