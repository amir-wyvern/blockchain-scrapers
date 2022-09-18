from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from concurrent.futures import ThreadPoolExecutor
from web3.middleware import geth_poa_middleware
from contracts import load_contracs
from dotenv import load_dotenv
from datetime import datetime
from web3 import Web3
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)-24s|%(levelname)-8s|%(lineno)-3d-%(name)s] > %(message)s',
    handlers=[
    logging.FileHandler("offlineScraper.log"),
    logging.StreamHandler()
    ]
)

class CONST :

    TOPICS = {
        '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' # Transfer
    }

    SWAP_FUNC_HASH = (
        '0x7ff36ab5',  #swapExactETHForTokens
        # '0x38ed1739',#swapExactTokensForTokens
        # '0x8803dbee',#swapTokensForExactTokens
        '0x4a25d94a',  #swapTokensForExactETH
        '0x18cbafe5',  #swapExactTokensForETH
        '0xfb3bdb41',  #swapETHForExactTokens
        # '0x5c11d795',#swapExactTokensForTokensSupportingFeeOnTransferTokens
        '0xb6f9de95',  #swapExactETHForTokensSupportingFeeOnTransferTokens
        '0x791ac947'   #swapExactTokensForETHSupportingFeeOnTransferTokens
    )

    REMOVE_FUNC_HASH = (
        '0x02751cec', # removeLiquidityETH 
        '0xded9382a', # removeLiquidityETHWithPermit
        '0xaf2979eb', # removeLiquidityETHSupportingFeeOnTransferTokens
        '0x5b0d5984', # removeLiquidityETHWithPermitSupportingFeeOnTransferTokens
    )

    ADDLIQ_FUNC_HASH = (
        '0xf305d719',
    )

    CREATE_FUNC_HASH = (
        '0x60806040',
    )

    SELL_TOKEN = 'sell'
    BUY_TOKEN = 'buy'


class Influx:

    def __init__(self, token: str, org: str, bucket: str, url: str, pool_size: int):

        self.org = org
        self.bucket = bucket
        self.url = url

        __influx = InfluxDBClient(url= self.url, token= token, org= self.org, pool_size= pool_size)
        self.__writeApi = __influx.write_api(write_options= ASYNCHRONOUS)

    def swap(self, timestamp, contract, sender, sawpType, value):
        
        logging.info(
            'swap      (constract:{0} |sender:{1} |time:{2} |value:{3} |sawpType:{4}'.format(
                contract,
                sender,
                timestamp,
                value,
                sawpType
            )
        )

        point = Point("swap") \
        .tag("swapType", sawpType) \
        .tag("contractAddress", contract) \
        .tag("sender", sender) \
        .field("amount",value) \
        .time(datetime.utcfromtimestamp(int(timestamp)))

        self.__writeApi.write(self.bucket, self.org, point)

    def created(self, timestamp, contract, sender, value):
        
        logging.info(
            'created   (constract:{0} |sender:{1} |time:{2} |value:{3}'.format(
                contract,
                sender,
                timestamp,
                value
            )
        )

        point = Point("created") \
        .tag("contractAddress", contract) \
        .tag("sender", sender) \
        .field("amount",value) \
        .time(datetime.utcfromtimestamp(int(timestamp)))

        self.__writeApi.write(self.bucket, self.org, point)

    def addLiqudity(self, timestamp, contract, sender, value):
        
        logging.info(
            'addLiq    (constract:{0} |sender:{1} |time:{2} |value:{3}'.format(
                contract,
                sender,
                timestamp,
                value
            )
        )
        point = Point("addLiqudity") \
        .tag("contractAddress", contract) \
        .tag("sender", sender) \
        .field("amount",value) \
        .time(datetime.utcfromtimestamp(int(timestamp)))

        self.__writeApi.write(self.bucket, self.org, point)

    def removeLiqudity(self, timestamp, contract, sender, value):

        logging.info(
            'removeLiq (constract:{0} |sender:{1} |time:{2} |value:{3}'.format(
                contract,
                sender,
                timestamp,
                value
            )
        )
        point = Point("removeLiqudity") \
        .tag("contractAddress", contract) \
        .tag("sender", sender) \
        .field("amount",value) \
        .time(datetime.utcfromtimestamp(int(timestamp)))

        self.__writeApi.write(self.bucket, self.org, point)


class FindTrasnferedValueInSwap:

    @staticmethod
    def swapExactETHForTokens(value, decoded_input):
        
        return value, CONST.BUY_TOKEN

    @staticmethod
    def swapETHForExactTokens(value, decoded_input):
        
        return value, CONST.BUY_TOKEN

    @staticmethod
    def swapExactTokensForETH(value, decoded_input):
        
        return decoded_input['amountOutMin'], CONST.SELL_TOKEN

    @staticmethod
    def swapTokensForExactETH(value, decoded_input):
        
        return decoded_input['amountOut'], CONST.SELL_TOKEN

    @staticmethod
    def swapExactETHForTokensSupportingFeeOnTransferTokens(value, decoded_input):
        
        return value, CONST.BUY_TOKEN

    @staticmethod
    def swapExactTokensForETHSupportingFeeOnTransferTokens(value, decoded_input):
        
        return decoded_input['amountOutMin'], CONST.SELL_TOKEN


class ScrapeNetwork:

    def __init__(self, influxObj: Influx, firstBlock: int, lastBlock: int, worker: int):

        self.firstBlock = firstBlock
        self.lastBlock = lastBlock
        self.influxObj = influxObj
        self.executor = ThreadPoolExecutor(max_workers= worker)

    def start(self):

        blockTxs = self.executor.map(self.getBlocks, range(self.firstBlock, self.lastBlock + 1))

        for blockTx in blockTxs:

            self.checkInputDataType(blockTx['transactions'], blockTx['timestamp'])

    def getBlocks(self, blockNumber):
        print('>',blockNumber)
        blockTxs = w3.eth.get_block(blockNumber, full_transactions= True)
        print('<',blockNumber)
        return blockTxs

    def checkInputDataType(self, txs, timestamp):

        for tx in txs:

            if tx['input'][:10] in CONST.SWAP_FUNC_HASH and \
               (contractAddresses := set( (decodeInput := routerContract.decode_function_input(tx['input']) )[1]['path'] ) & contractCache) :

                for _ in range(len(contractAddresses)):
                    contractAddress = contractAddresses.pop()

                nameFunction = decodeInput[0].fn_name
                transferedValue, sawpType = getattr(
                    FindTrasnferedValueInSwap,
                    nameFunction)(tx['value'], decodeInput[1])
                    
                transferedValue = round(transferedValue / 10**18 ,5) 
                sender = tx['from']

                self.influxObj.swap(
                    timestamp= timestamp,
                    contract= contractAddress,
                    sender= sender,
                    sawpType= sawpType,
                    value= transferedValue
                    )

            elif tx['input'][:10] in CONST.ADDLIQ_FUNC_HASH and \
                ( decodeInput := routerContract.decode_function_input(tx['input']) )[1]['token'] in contractCache:
                
                contractAddress = decodeInput[1]['token']
                transferedValue = round(tx['value']/10**18, 5)
                sender = tx['from']

                self.influxObj.addLiqudity(
                    timestamp= timestamp,
                    contract= contractAddress,
                    sender= sender,
                    value= transferedValue
                    )

            elif tx['input'][:10] in CONST.REMOVE_FUNC_HASH and \
                (decodeInput := routerContract.decode_function_input(tx['input']) )[1]['token'] in contractCache :
                
                contractAddress = decodeInput[1]['token']
                transferedValue = decodeInput[1]['amountETHMin']
                transferedValue = round(transferedValue/10**18, 5)
                sender = tx['from']

                self.influxObj.removeLiqudity(
                    timestamp= timestamp,
                    contract= contractAddress,
                    sender= sender,
                    value= transferedValue
                    )

            elif tx['input'][:10] in CONST.CREATE_FUNC_HASH :
                
                txData = w3.eth.get_transaction_receipt(tx['hash'].hex())
                topicLogs = set([log['topics'][0].hex() for log in txData['logs']])
                
                if topicLogs and not(CONST.TOPICS - topicLogs):

                    contractAddress = txData['contractAddress']
                    contractCache.add(contractAddress)

                    sender = tx['from']
                    transferedValue = tx['value']
                    transferedValue = round(transferedValue/10**18, 5)

                    self.influxObj.created(
                        timestamp= timestamp,
                        contract= contractAddress,
                        sender= sender,
                        value= transferedValue
                        )



if __name__ == '__main__':

    contractCache = set()

    w3 = Web3(Web3.HTTPProvider("https://bsc-dataseed.binance.org") )
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    contracts = load_contracs()
    routerContract = w3.eth.contract(
        address=Web3.toChecksumAddress('0x10ED43C718714eb63d5aA57B78B54704E256024E'),
        abi=contracts['router']['abi']
    )

    load_dotenv("../.env")

    org = 'org'
    bucket = 'BSC_Scraping'
    url = 'http://localhost:8086'
    token = os.getenv('INFLUX_TOKEN')

    startBlock = 21061407
    lastBlock = 21161407
    workers = 10

    influxObj = Influx(token, org, bucket, url, pool_size= workers + 5)
    ScrapeNetwork(influxObj, startBlock, lastBlock, workers).start()





