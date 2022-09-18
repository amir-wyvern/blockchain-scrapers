import json
import glob

def load_contracs():
    ls_contracts = {}
    for _file in glob.glob('../abi/contract-*.json'):
        with open(_file ,'r') as fi :
            fileData = json.load(fi)
            ls_contracts[fileData['name']] = fileData
        
    return ls_contracts
