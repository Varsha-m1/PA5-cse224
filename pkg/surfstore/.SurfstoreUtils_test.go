package surfstore

import (
	"fmt"
	"log"
)

// Implement the logic for a client syncing with the server here.
func ClientSync_test(client RPCClient) {

	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	log.Println(blockStoreAddr)

	blockSize := client.BlockSize
	// baseDir := client.BaseDir
	// metaAddr := client.MetaStoreAddr

	block := &Block{
		BlockData: []byte("taru"),
		BlockSize: int32(blockSize),
	}

	succ := new(bool)
	if err := client.PutBlock(block, blockStoreAddr, succ); err != nil || !*succ {
		fmt.Printf("did not put block: %v", err)
	}

	block = &Block{
		BlockData: []byte("abcd"),
		BlockSize: int32(blockSize),
	}
	if err := client.PutBlock(block, blockStoreAddr, succ); err != nil || !*succ {
		fmt.Printf("did not put block: %v", err)
	}

	block = &Block{
		BlockData: []byte("wxyz"),
		BlockSize: int32(blockSize),
	}
	if err := client.PutBlock(block, blockStoreAddr, succ); err != nil || !*succ {
		fmt.Printf("did not put block: %v", err)
	}

	hashes_in := make([]string, 0)

	blockHash := GetBlockHashString([]byte("abcd"))
	hashes_in = append(hashes_in, blockHash)

	block_return := &Block{}

	if err := client.GetBlock(blockHash, blockStoreAddr, block_return); err != nil {
		log.Fatal(err)
	}

	log.Println("Returned block")
	log.Println(block_return.String())

	blockHash = GetBlockHashString([]byte("wxyz"))
	hashes_in = append(hashes_in, blockHash)

	blockHash = GetBlockHashString([]byte("hijk"))
	hashes_in = append(hashes_in, blockHash)

	hashes_out := make([]string, 0)
	if err := client.HasBlocks(hashes_in, blockStoreAddr, &hashes_out); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Input hash: ", hashes_in)
	fmt.Println("Output hash: ", hashes_out)

	// metastore test

	metaData := &FileMetaData{
		Filename:      "abcd.txt",
		Version:       1,
		BlockHashList: hashes_in,
	}

	fileMap := make(map[string]*FileMetaData)
	fileMap["abcd.txt"] = metaData

	version := new(int32)
	if err := client.UpdateFile(metaData, version); err != nil {
		fmt.Println("Current version number is: ", *version)
		log.Fatal(err)
	}

	fmt.Println("Current version number is: ", *version)

	getFileMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&getFileMap); err != nil {
		log.Fatal(err)
	}

	PrintMetaMap(getFileMap)
}
