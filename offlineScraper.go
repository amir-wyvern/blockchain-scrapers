package main

import (
    "context"
    "fmt"
    "log"
    "math/big"
    "sync"
    "time"
    "bytes"
    "os"
	"encoding/json"
	"io/ioutil"
    "github.com/thedevsaddam/iter"
	"github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/ethclient"
    "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/accounts/abi"  
    "github.com/influxdata/influxdb-client-go/v2"
    "github.com/joho/godotenv"
)

var TOPICS [1]common.Hash

var WHITE_LIST_ADDRESS map[common.Address]bool 
var BLACK_LIST_ADDRESS map[common.Address]bool
var CLIENT *ethclient.Client
var NONE_ADDRESS common.Address

var INFLUX_CLI = influxdb2.NewClient("","") 

var CREATE = map[[4]byte]interface{} {
    [4]byte{0x60, 0x80, 0x60, 0x40} : func (_ *big.Int, _ map[string]interface{})(*big.Int,string){return big.NewInt(0), ""},
}
var ADD_LIQ = map[[4]byte]interface{} {
	[4]byte{0xf3, 0x05, 0xd7, 0x19} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, ""} ,
}
var SWAP = map[[4]byte]interface{} {
	[4]byte{0x7f, 0xf3, 0x6a, 0xb5} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, "buy"} ,
	[4]byte{0xfb, 0x3b, 0xdb, 0x41} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, "buy"} ,
	[4]byte{0xb6, 0xf9, 0xde, 0x95} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, "buy"} ,
	[4]byte{0x4a, 0x25, 0xd9, 0x4a} : func(_ *big.Int, inputABI map[string]interface{})(*big.Int,string){return inputABI["amountOut"].(*big.Int), "sell"},
	[4]byte{0x18, 0xcb, 0xaf, 0xe5} : func(_ *big.Int, inputABI map[string]interface{})(*big.Int,string){return inputABI["amountOutMin"].(*big.Int), "sell"} ,
	[4]byte{0x79, 0x1a, 0xc9, 0x47} : func(_ *big.Int, inputABI map[string]interface{})(*big.Int,string){return inputABI["amountOutMin"].(*big.Int), "sell"} ,
}
var REMOVE_LIQ = map[[4]byte]interface{} {
	[4]byte{0x02, 0x75, 0x1c, 0xec} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, ""} ,
	[4]byte{0xaf, 0x29, 0x79, 0xeb} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, ""} ,
	[4]byte{0xde, 0xd9, 0x38, 0x2a} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, ""} ,
	[4]byte{0x5b, 0x0d, 0x59, 0x84} : func(value *big.Int, _ map[string]interface{})(*big.Int,string){return value, ""} ,
}


var CONTRACT_ABI = GetContractABI()
func GetContractABI() *abi.ABI {

	jsonFile, err := os.Open("contract-router-pancake.json")

	if err != nil {
        fmt.Println(err)
    }
    defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	
	var RawABI map[string]interface{}
    json.Unmarshal([]byte(byteValue), &RawABI)

	byteABI ,_:= json.Marshal(RawABI["abi"])
	
	reader := bytes.NewReader(byteABI)
	buf := make([]byte, len(byteABI))
	_, err2 := reader.Read(buf)
	if err2 != nil {
	  log.Fatal(err2)
	}
	
	contractABI, err := abi.JSON(bytes.NewReader(buf))
	if err != nil {
		log.Fatal(err)
	}

	return &contractABI
}

func DecodeTransactionInputData(contractABI *abi.ABI, data []byte) map[string]interface{} {
	
    methodSigData := data[:4]
	inputsSigData := data[4:]
	method, err := contractABI.MethodById(methodSigData)
	if err != nil {
		log.Fatal(err)
	}
	inputsMap := make(map[string]interface{})
	if err := method.Inputs.UnpackIntoMap(inputsMap, inputsSigData); err != nil {
		log.Fatal(err)
	} 
	return inputsMap
}

// ============= part 3
func spinupWorkerForReCheckTx(count int, TxPipline chan StructTxPipline, reChackTxPipline <-chan StructTxPipline) {    
    
    // cacheTx := make(map[common.Hash]bool)
    var stateFlag bool = false

    go func (){
        for true {
            if len(reChackTxPipline) > 60 {
                stateFlag = false
                
            } else if len(reChackTxPipline) < 10 {
                stateFlag = true
                for i := 0; i < count; i++ {
                    go func (workerId int) {
                        for txWithBlockTime := range reChackTxPipline { 
                            if stateFlag == false{
                                return
                            }
                            TxPipline <- txWithBlockTime
                            // analyzeTx(txWithBlockTime, reChackTxPipline)
                        }
                    }(i)
                }
            }
            time.Sleep(time.Second / 2)
        }
    }()
    
}

// ============= end

// ============= part 2
func extractContractAddress(listAddress []common.Address) common.Address {
    for _, address := range listAddress {
        if WHITE_LIST_ADDRESS[address] {
            return address
        }
    }
    return NONE_ADDRESS
}
func isContainTopicsHash(logs []*types.Log) bool {
    for _, log := range logs {
        for _, topic := range log.Topics {
            for _, hash := range TOPICS {
                if hash == topic {
                    return true
                }
            }
        }
    }
    return false 
}

func spinupWorkerForGetTx(count int, TxPipline <-chan StructTxPipline, reChackTxPipline chan StructTxPipline) {
    for i := 0; i < count; i++ {
        go func (workerId int) {
            for txWithBlockTime := range TxPipline { 
                analyzeTx(txWithBlockTime, reChackTxPipline)
            }
        }(i)
    }
}

func swapTx(txWithBlockTime StructTxPipline, reChackTxPipline chan StructTxPipline) {

    // blockTime := txWithBlockTime.blockTime
    tx := txWithBlockTime.tx

    key4Byte := [4]byte{tx.Data()[0], tx.Data()[1], tx.Data()[2], tx.Data()[3]} 

    inputABI := DecodeTransactionInputData(CONTRACT_ABI, tx.Data()) 
    contactAddress := extractContractAddress(inputABI["path"].([]common.Address))
    if contactAddress != NONE_ADDRESS  {
        value, _ := SWAP[key4Byte].(func(*big.Int, map[string]interface{})(*big.Int,string))(tx.Value(), inputABI)
        // from, _ := types.Sender(types.NewEIP155Signer(tx.ChainId()), tx) 
        fmt.Println("SWAP : " , contactAddress ,value)

    } else {
        fmt.Println("swap not in white list")
        reChackTxPipline <- txWithBlockTime
    }
}

func createTx(txWithBlockTime StructTxPipline) {
    
    // blockTime := txWithBlockTime.blockTime
    tx := txWithBlockTime.tx

    key4Byte := [4]byte{tx.Data()[0], tx.Data()[1], tx.Data()[2], tx.Data()[3]} 

    inputABI := make(map[string]interface{})
    value, _ := CREATE[key4Byte].(func(*big.Int,map[string]interface{})(*big.Int,string))(tx.Value(), inputABI) 
    from, _ := types.Sender(types.NewEIP155Signer(tx.ChainId()), tx) 
    receipt, _ := CLIENT.TransactionReceipt(context.Background(), tx.Hash())
    if isContainTopicsHash(receipt.Logs) {
        fmt.Println("=CRAETE : " ,receipt.ContractAddress ,from ,value ) 
        WHITE_LIST_ADDRESS[receipt.ContractAddress] = true
    }
}

func addLiquidityTx(txWithBlockTime StructTxPipline, reChackTxPipline chan StructTxPipline) {
    
    // blockTime := txWithBlockTime.blockTime
    tx := txWithBlockTime.tx

    key4Byte := [4]byte{tx.Data()[0], tx.Data()[1], tx.Data()[2], tx.Data()[3]} 

    inputABI := DecodeTransactionInputData(CONTRACT_ABI, tx.Data())
    if WHITE_LIST_ADDRESS[inputABI["token"].(common.Address)] {
        value, _ := ADD_LIQ[key4Byte].(func(*big.Int, map[string]interface{})(*big.Int,string))(tx.Value(), inputABI) 
        fmt.Println("ADD_LIQ : " , inputABI["token"] ,value)
    } else {
        fmt.Println("addLiq not in white list")
        reChackTxPipline <- txWithBlockTime
    }
    
}

func removeLiquidityTx(txWithBlockTime StructTxPipline, reChackTxPipline chan StructTxPipline) {

    // blockTime := txWithBlockTime.blockTime
    tx := txWithBlockTime.tx
    
    key4Byte := [4]byte{tx.Data()[0], tx.Data()[1], tx.Data()[2], tx.Data()[3]} 

    inputABI := DecodeTransactionInputData(CONTRACT_ABI, tx.Data())
    if WHITE_LIST_ADDRESS[inputABI["token"].(common.Address)] {
        value, _ := REMOVE_LIQ[key4Byte].(func(*big.Int, map[string]interface{})(*big.Int,string))(tx.Value(), inputABI) 
        fmt.Println("REMOVE_LIQ : " , inputABI["token"] ,value)
    } else {
        fmt.Println("removeLiq not in white list")
        reChackTxPipline <- txWithBlockTime
    }
}

func analyzeTx(txWithBlockTime StructTxPipline, reChackTxPipline chan StructTxPipline) {

    tx := txWithBlockTime.tx
    key4Byte := [4]byte{tx.Data()[0], tx.Data()[1], tx.Data()[2], tx.Data()[3]} 

    if SWAP[key4Byte] != nil {
        swapTx(txWithBlockTime, reChackTxPipline)
        
    } else if CREATE[key4Byte] != nil {
        createTx(txWithBlockTime)
        
    } else if ADD_LIQ[key4Byte] != nil {
        addLiquidityTx(txWithBlockTime, reChackTxPipline)

    } else if REMOVE_LIQ[key4Byte] != nil {
        removeLiquidityTx(txWithBlockTime, reChackTxPipline)

    }
}
// ============== end

// ============== part 1
func getBlockNumber(number int) (types.Transactions, uint64) {

    fmt.Printf("> %d \n", number)

    blockNumber := big.NewInt(int64(number))
    block, err := CLIENT.BlockByNumber(context.Background(), blockNumber)

    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf( "<%d \n" ,block.Number().Uint64())

    return block.Transactions(), block.Header().Time
}

func sendTxToPipline(blockTxs types.Transactions, blockTime uint64, TxPipline chan StructTxPipline) {

    for _, tx := range blockTxs {
        if len(tx.Data()) >= 4 {
            txWithBlockTime := StructTxPipline{tx:tx ,blockTime:blockTime }
            TxPipline <- txWithBlockTime
        }
    }
}

func spinupWorkerForGetBlock(count int, blockNumberPipline <-chan int, TxPipline chan StructTxPipline, wg *sync.WaitGroup) {

    for i := 0; i < count; i++ {
        wg.Add(1)
        go func (workerId int) {
            for blockNumber := range blockNumberPipline {
                blockTxs, blockTime := getBlockNumber(blockNumber)
                sendTxToPipline(blockTxs, blockTime, TxPipline)
            }
            wg.Done()
        }(i)
    }
}
// ============== end

type StructTxPipline struct {
    tx *types.Transaction
    blockTime uint64
}

func main() {

    start := time.Now()
    
    err := godotenv.Load()
    if err != nil {
      log.Fatal("Error loading .env file")
    }
    
    WHITE_LIST_ADDRESS = map[common.Address]bool{}
    CLIENT, _ = ethclient.Dial("https://bsc-dataseed.binance.org")
    NONE_ADDRESS = common.HexToAddress("0x0000000000000000000000000000000000000000")
    influxToken := os.Getenv("TOEKN") 
    INFLUX_CLI = influxdb2.NewClient("http://localhost:8086", influxToken ) 
    TOPICS[0] = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

    wg := &sync.WaitGroup{}

    // numberOfWorkers := 100

    blockNumberPipline := make(chan int)
    reChackTxPipline := make(chan StructTxPipline)
    TxPipline := make(chan StructTxPipline)

    spinupWorkerForGetBlock(1, blockNumberPipline, TxPipline, wg) 
    spinupWorkerForGetTx(1, TxPipline, reChackTxPipline) 
    spinupWorkerForReCheckTx(1, TxPipline, reChackTxPipline) 
    
    for i := range iter.N(21061418,21071407) {
        blockNumberPipline <- i
    }

    close(blockNumberPipline)
    wg.Wait()
    elapsed := time.Since(start)
    fmt.Printf("Binomial took %s", elapsed)

}