package main

func main(){
	checkpointDb := MustOpenPebbleDB(fmt.Sprintf("db/%s/checkpoint", "mainnet26"))
	ledgerDb := MustOpenPebbleDB(fmt.Sprintf("db/%s/ledger", "mainnet26"))




}
