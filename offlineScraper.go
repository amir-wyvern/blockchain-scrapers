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
var WORKER_NUMBER int
var WHITE_LIST_ADDRESS sync.Map // use of sync.map instead of it
var BLACK_LIST_ADDRESS sync.Map
var CLIENT *ethclient.Client
var NONE_ADDRESS common.Address

var INFLUX_CLI = influxdb2.NewClient("","") 

var CREATE = map[[4]byte]interface{} {
    [4]byte{0x60, 0x80, 0x60, 0x40} : func (_ *big.Int, tx *types.Transaction)(*big.Int,string){return big.NewInt(0), ""},
}
var ADD_LIQ = map[[4]byte]interface{} {
	[4]byte{0xf3, 0x05, 0xd7, 0x19} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, ""} ,
}
var SWAP = map[[4]byte]interface{} {
	[4]byte{0x7f, 0xf3, 0x6a, 0xb5} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, "buy"} ,
	[4]byte{0xfb, 0x3b, 0xdb, 0x41} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, "buy"} ,
	[4]byte{0xb6, 0xf9, 0xde, 0x95} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, "buy"} ,
	[4]byte{0x4a, 0x25, 0xd9, 0x4a} : func(_ *big.Int, tx *types.Transaction)(*big.Int,string){return DecodeTransactionInputData(CONTRACT_ABI, tx.Data())["amountOut"].(*big.Int), "sell"},
	[4]byte{0x18, 0xcb, 0xaf, 0xe5} : func(_ *big.Int, tx *types.Transaction)(*big.Int,string){return DecodeTransactionInputData(CONTRACT_ABI, tx.Data())["amountOutMin"].(*big.Int), "sell"} ,
	[4]byte{0x79, 0x1a, 0xc9, 0x47} : func(_ *big.Int, tx *types.Transaction)(*big.Int,string){return DecodeTransactionInputData(CONTRACT_ABI, tx.Data())["amountOutMin"].(*big.Int), "sell"} ,
}
var REMOVE_LIQ = map[[4]byte]interface{} {
	[4]byte{0x02, 0x75, 0x1c, 0xec} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, ""} ,
	[4]byte{0xaf, 0x29, 0x79, 0xeb} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, ""} ,
	[4]byte{0xde, 0xd9, 0x38, 0x2a} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, ""} ,
	[4]byte{0x5b, 0x0d, 0x59, 0x84} : func(value *big.Int, tx *types.Transaction)(*big.Int,string){return value, ""} ,
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
func spinupWorkerForReCheckTx(
    count int,
    TxPipline chan StructTxPipline,
    reChackTxPipline <-chan StructTxPipline,
    currentBlockNumberPipline <-chan int) {    
    
    var stateFlag bool = false

    filterTx := func(txWithBlockTime StructTxPipline) {
        tx := txWithBlockTime.tx
        key4Byte := [4]byte{tx.Data()[0], tx.Data()[1], tx.Data()[2], tx.Data()[3]} 
        
        txForm, _ := formingTx(key4Byte)
        contactAddress := txForm.ContractAddress(tx)

        if contactAddress != NONE_ADDRESS  {
            fmt.Println("========================== : " ,txWithBlockTime.tx.Hash().Hex())
            TxPipline <- txWithBlockTime

        }else {
            _, exist := BLACK_LIST_ADDRESS.Load(contactAddress)
            if !exist {
                fmt.Println("!!!!!!!!!!!!!!!!!!!! : " ,txWithBlockTime.tx.Hash().Hex())
                BLACK_LIST_ADDRESS.Store(contactAddress, true)
            }
        }
    }

    fn := func() { 

        for txWithBlockTime := range reChackTxPipline { 
            if stateFlag == false{
                return
            }
            filterTx()
            // analyzeTx(txWithBlockTime, reChackTxPipline)
        }
    }

    go func () {
        var temp int
        temp <- currentBlockNumberPipline
        for blockNumber := range currentBlockNumberPipline{
            if blockNumber - temp >= 200 {
                temp = blockNumber
                stateFlag = true

                for i := 0; i < count; i++ {
                    go fn()
                }
            }
            // fmt.Println("block current : " ,x)
        }
    }()

    go func (){
        for true {
            fmt.Println("len : " , len(reChackTxPipline))
            if len(reChackTxPipline) > 300 && stateFlag == false {
                go func() {
                    txWithBlockTime <- reChackTxPipline
                    filterTx(txWithBlockTime)
                }

                // fmt.Println("\n#### > 60\n")
                // stateFlag = true
                // for i := 0; i < count; i++ {
                //     go fn()
                // }
                
            } else if len(reChackTxPipline) < 5 && stateFlag == true{
                fmt.Println("\n#### < 10\n")

                stateFlag = false

            }
            time.Sleep(time.Second / 2)
        }
    }()
    
}

// ============= end

// ************* part 2
func extractAddressFromSwap(tx *types.Transaction) common.Address {

    inputABI := DecodeTransactionInputData(CONTRACT_ABI, tx.Data()) 

    contactAddress := NONE_ADDRESS
    for _, address := range inputABI["path"].([]common.Address) {
        _, exist := WHITE_LIST_ADDRESS.Load(address)
        if exist{
            contactAddress = address
            break
        }
    }

    return contactAddress
}
func extractAddressFromAddLiqudity(tx *types.Transaction) common.Address {
    inputABI := DecodeTransactionInputData(CONTRACT_ABI, tx.Data())
    contactAddress := inputABI["token"].(common.Address)
    
    return contactAddress
}
func extractAddressFromRemoveLiqudity(tx *types.Transaction) common.Address {
    inputABI := DecodeTransactionInputData(CONTRACT_ABI, tx.Data())
    contactAddress := inputABI["token"].(common.Address)
    
    return contactAddress
}
func extractAddressFromCreate(tx *types.Transaction) common.Address {
    receipt, _ := CLIENT.TransactionReceipt(context.Background(), tx.Hash())
    return receipt.ContractAddress
}


func formingTxForInflux(
    mem string,
    contractAddress common.Address,
    sender common.Address,
    swapType string,
    amount float32,
    time time.Time ) {
    
    writeAPI := INFLUX_CLI.WriteAPI("org", "BSC_Scraping")
    point :=influxdb2.NewPointWithMeasurement(mem).
        AddTag("contractAddress", contractAddress.Hex()).
        AddTag("sender", sender.Hex()).
        AddTag("swapType", swapType).
        AddField("amount", amount).
        SetTime(time)
    
    writeAPI.WritePoint(point)
}

type TxFunctions struct {
    txType string
    ValueAndSwapType map[[4]byte]interface{}
    ContractAddress func(*types.Transaction)common.Address
    SendToInflux func(string,common.Address,common.Address,string,float32,time.Time)
}

func formingTx(key4Byte [4]byte) (TxFunctions, bool) {
    
    formResponse := TxFunctions{
        SendToInflux: formingTxForInflux,
    }

    if SWAP[key4Byte] != nil {

        formResponse.ContractAddress = extractAddressFromSwap
        formResponse.ValueAndSwapType = SWAP
        formResponse.txType = "swap"
        return formResponse, false
        
    } else if CREATE[key4Byte] != nil {
        
        formResponse.ContractAddress = extractAddressFromCreate
        formResponse.ValueAndSwapType = CREATE
        formResponse.txType = "create"
        return formResponse, false

    } else if ADD_LIQ[key4Byte] != nil {
        
        formResponse.ContractAddress = extractAddressFromAddLiqudity
        formResponse.ValueAndSwapType = ADD_LIQ
        formResponse.txType = "addLiquidity"
        return formResponse, false
    
    } else if REMOVE_LIQ[key4Byte] != nil {
        
        formResponse.ContractAddress = extractAddressFromRemoveLiqudity
        formResponse.ValueAndSwapType = REMOVE_LIQ
        formResponse.txType = "removeLiquidity"
        return formResponse, false

    } else {
        return TxFunctions{}, true
    }
}

func analyzeTx(txWithBlockTime StructTxPipline, reChackTxPipline chan StructTxPipline ) {

    tx := txWithBlockTime.tx
    key4Byte := [4]byte{tx.Data()[0], tx.Data()[1], tx.Data()[2], tx.Data()[3]} 

    txForm, err := formingTx(key4Byte)

    if !err {
        // sender, _ := types.Sender(types.NewEIP155Signer(tx.ChainId()), tx)
        value, _ := txForm.ValueAndSwapType[key4Byte].(func(*big.Int, *types.Transaction)(*big.Int,string))(tx.Value(), tx)
        
        if key4Byte == [4]byte{0x60, 0x80, 0x60, 0x40} {
            receipt, _ := CLIENT.TransactionReceipt(context.Background(), tx.Hash())
            if isContainTopicsHash(receipt.Logs) {

                WHITE_LIST_ADDRESS.Store(receipt.ContractAddress, true)
                // txForm.formingTxForInflux(txForm.txType ,...)
            }

        } else {
            contractAddress := txForm.ContractAddress(tx)
            _, exist := WHITE_LIST_ADDRESS.Load(contractAddress)
            if exist {
                fmt.Println(txForm.txType , " : " ,value)
                // txForm.formingTxForInflux(txForm.txType,...)
            } else {
                _, exist := BLACK_LIST_ADDRESS.Load(contractAddress)
                if !exist {
                    reChackTxPipline <- txWithBlockTime
                }
            }
        }
    }
    
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

// ************* end

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

func spinupWorkerForGetBlock(
    count int,
    blockNumberPipline <-chan int,
    TxPipline chan StructTxPipline,
    currentBlockNumberPipline chan int,
    wg *sync.WaitGroup,) {

    for i := 0; i < count; i++ {
        wg.Add(1)
        go func (workerId int) {
            for blockNumber := range blockNumberPipline {
                blockTxs, blockTime := getBlockNumber(blockNumber)
                sendTxToPipline(blockTxs, blockTime, TxPipline)
                currentBlockNumberPipline <- blockNumber
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
    
    WORKER_NUMBER = 100

    CLIENT, _ = ethclient.Dial("https://bsc-dataseed.binance.org")
    NONE_ADDRESS = common.HexToAddress("0x0000000000000000000000000000000000000000")
    influxToken := os.Getenv("TOEKN") 
    INFLUX_CLI = influxdb2.NewClient("http://localhost:8086", influxToken ) 
    TOPICS[0] = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

    wg := &sync.WaitGroup{}

    // numberOfWorkers := 100

    blockNumberPipline := make(chan int)
    reChackTxPipline := make(chan StructTxPipline,500)
    TxPipline := make(chan StructTxPipline)
    
    currentBlockNumberPipline := make(chan int)

    spinupWorkerForGetBlock(100, blockNumberPipline, TxPipline, currentBlockNumberPipline, wg) 
    spinupWorkerForGetTx(10, TxPipline, reChackTxPipline) 
    spinupWorkerForReCheckTx(3, TxPipline, reChackTxPipline, currentBlockNumberPipline) 
    
    for i := range iter.N(21061418,21071407) {
        blockNumberPipline <- i
    }

    close(blockNumberPipline)
    wg.Wait()
    elapsed := time.Since(start)
    fmt.Printf("Binomial took %s", elapsed)

}