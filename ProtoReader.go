//adhoc team problem outline: https://github.com/adhocteam/homework/tree/master/proto

package main 

import (
	"encoding/binary"
	"bytes"
	"fmt"
	"os"
	"io"
)

func main() {
	var i uint32
	totalDebits := AccountBalance{userID: 0, actType: int32(0)}
	totalCredits := AccountBalance{userID: 0, actType: int32(1)}
	vipAct := AccountBalance{userID: uint64(2456938384156277127), actType: int32(4)}
	var autoPayStarted int = 0
	var autoPayEnded int = 0

	f, err := os.Open("txnlog.dat")
	check(err)
	
	//read the header
	recs := readHeader(f)
	for  i = 0; i <= recs; i++ {	
		//read the record value
		r := readRecord(f)
		
		if r.recordType == int32(0){
			totalDebits.balance += r.cashValue
			if r.userID == vipAct.userID {
				vipAct.balance += r.cashValue
			}
		} else if r.recordType == int32(1) {
			totalCredits.balance += r.cashValue
			if r.userID == vipAct.userID {
				vipAct.balance -= r.cashValue
			}
		} else if r.recordType == int32(2){
			autoPayStarted++
		} else if r.recordType == int32(3){
			autoPayEnded++
		}
	}

	f.Close()

	fmt.Println("Dollars in the debits column: ", totalCredits.balance)
	fmt.Println("Dollars in the credits column: ", totalDebits.balance)
	fmt.Println("autopays started: ", autoPayStarted)
	fmt.Println("autopays ended: ", autoPayEnded)
	fmt.Println("2456938384156277127 user balance: ", vipAct.balance)
}

func check(e error) {
	if e != nil {
		if e == io.EOF {
			//fmt.Printf("EOF reached")
			//os.Exit(0)
		}
		fmt.Println(e)
		os.Exit(1)
	}
}
type Record struct {
	recordType	int32
	userID		uint64
	cashValue	float64
}

type AccountBalance struct {
	userID	uint64
	actType	int32 
	balance float64
}

func readHeader(f *os.File) (recs uint32) {
	proto := make([]byte, 4)
	ver := make([]byte, 1)
	numRecs := make([]byte,4)

	_,err := f.Read(proto)
	check(err)

	_,err = f.Read(ver)

	_,err = f.Read(numRecs)
	check(err)
	numRecs32 := binary.BigEndian.Uint32(numRecs)

	return numRecs32
}

func readRecord(f *os.File) (r Record) {
	typeRec := make([]byte, 1)
	time := make([]byte, 4)
	uid := make([]byte, 8)
	amt := make([]byte, 8)

	_,err := f.Read(typeRec)
	check(err)

	_,err = f.Read(time)
	check(err)

	_,err = f.Read(uid)
	check(err)
	uid64 := binary.BigEndian.Uint64(uid)

	//if the record is a debit or a credit
	if (typeRec[0] == 0x0) || (typeRec[0] == 0x1) {
		_,err = f.Read(amt)
		check(err)
		var cash float64
		buf := bytes.NewReader(amt)
		err = binary.Read(buf, binary.BigEndian, &cash)
		basicRecord := Record{recordType: int32(typeRec[0]), userID: uid64, cashValue: cash}
		return basicRecord
	} else {
		autopayRecord := Record{recordType: int32(typeRec[0]), userID: uid64}
		return autopayRecord
	}
}