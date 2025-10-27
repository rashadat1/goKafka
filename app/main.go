package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"slices"
)

type ApiData struct {
	ApiVersions []int16
	MinVersion  int16
	MaxVersion  int16
}

var allApiData map[int16]ApiData // slice of int16 versions that are valid per api key

func main() {
	// 18 is for the ApiVersions api - its valid versions are 0, 1, 2, 3, 4
	// TODO:  maybe we can put this in another file
	allApiData = make(map[int16]ApiData)

	allApiData[int16(18)] = ApiData{
		ApiVersions: []int16{int16(0), int16(1), int16(2), int16(3), int16(4)},
		MinVersion:  int16(0),
		MaxVersion:  int16(4),
	}

	ln, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Fatalf("Error initiating tcp server listening on port 9092: %v\n", err)
	}
	fmt.Println("Successfully created server listening on port 9092...")

	conn, err := ln.Accept()
	if err != nil {
		log.Fatalf("Error accepting connection made to server: %v\n", err)
	}
	correlation_id := int32(7)
	msg_size := int32(0)
	error_code := int16(0)

	msgSizeBytes := make([]byte, 4)
	_, err = conn.Read(msgSizeBytes)
	fmt.Printf("MsgSizeBytes: %v\n", msgSizeBytes)
	if err != nil {
		log.Fatalf("Error reading bytes from client: %v\n", err)
	}
	msg_size, err = getInt32FromBytes(msgSizeBytes)
	if err != nil {
		log.Printf("Failed parsing the message size from the client request: %v\n", err)
	}
	if msg_size < 0 {
		log.Printf("Message size is invalid signed int value: %d\n", msg_size)
	}
	fullBody := make([]byte, msg_size)
	bytesRead := 0
	for {
		if int32(bytesRead) == msg_size {
			break
		}
		n, err := conn.Read(fullBody)
		if err != nil {
			log.Printf("Failed to read bytes from channel: %v\n", err)
			break
		}
		bytesRead += n
	}
	if msg_size <= 0 || int32(bytesRead) != msg_size {
		log.Printf("Failed to parse the expected number of bytes in request - parsed: %d, expected: %d\n", bytesRead, msg_size)
		failBuf := new(bytes.Buffer)
		binary.Write(failBuf, binary.BigEndian, msg_size)
		binary.Write(failBuf, binary.BigEndian, correlation_id)
		conn.Write(failBuf.Bytes())
		return
	}
	// read expected number of bytes without issue
	// first read the request api key and api version

	request_api_key, err := getInt16FromBytes(fullBody[0:2])
	request_api_version, err := getInt16FromBytes(fullBody[2:4])
	// for now we will not validate the api key
	if !isValidApiVersion(request_api_key, request_api_version) {
		error_code = int16(35)
	}
	// get correlation_id which is 4 bytes starting at the 9th
	correlation_id_bytes := fullBody[4:8]
	correlation_id, err = getInt32FromBytes(correlation_id_bytes)
	if err != nil {
		log.Printf("Failed parsing correlation id, request id, or api key bytes from the client request: %v\n", err)
	}
	log.Printf("Correlation id bytes: %v\n", correlation_id_bytes)
	log.Printf("Correlation id int32: %v\n", correlation_id)

	bodyBuf := new(bytes.Buffer)
	err = binary.Write(bodyBuf, binary.BigEndian, correlation_id)
	if err != nil {
		log.Printf("Failed to write correlation id to buffer: %v\n", err)
	}
	err = binary.Write(bodyBuf, binary.BigEndian, error_code)
	if err != nil {
		log.Printf("Failed to write error code to buffer: %v\n", err)
	}
	if error_code == int16(0) {
		switch request_api_key {
		case int16(18):
			err = constructApiVersionResponse(bodyBuf, allApiData)
			if err != nil {
				log.Printf("Failed to write api version bytes to buffer: %v\n", err)
			}
		default:
			log.Printf("Api Key case not implemented yet: %v", request_api_key)
		}
	}
	response_size := int32(len(bodyBuf.Bytes()))
	fullBuf := new(bytes.Buffer)
	err = binary.Write(fullBuf, binary.BigEndian, response_size)
	fullBuf.Write(bodyBuf.Bytes())
	log.Printf("Bytes in buffer: %v\n", fullBuf.Bytes())
	conn.Write(fullBuf.Bytes())
}
func getInt32FromBytes(byteSlice []byte) (int32, error) {
	var res int32
	buf := bytes.NewReader(byteSlice)

	err := binary.Read(buf, binary.BigEndian, &res)
	return res, err
}
func getInt16FromBytes(byteSlice []byte) (int16, error) {
	var res int16
	buf := bytes.NewReader(byteSlice)

	err := binary.Read(buf, binary.BigEndian, &res)
	return res, err
}
func isValidApiVersion(request_api_key, request_api_version int16) bool {
	// given the api key we need to determine if the given api version is valid
	// for each api key - there is a set of valid api versions
	apiData := allApiData[request_api_key]
	return slices.Contains(apiData.ApiVersions, request_api_version)

}
func constructApiVersionResponse(buf *bytes.Buffer, allApiData map[int16]ApiData) error {
	num_api_keys := int8(len(allApiData) + 1)

	err := binary.Write(buf, binary.BigEndian, num_api_keys)
	if err != nil {
		return err
	}
	for apiKey, value := range allApiData {
		err = binary.Write(buf, binary.BigEndian, apiKey)
		err = binary.Write(buf, binary.BigEndian, value.MinVersion)
		err = binary.Write(buf, binary.BigEndian, value.MaxVersion)
		err = binary.Write(buf, binary.BigEndian, int8(0)) // tag buffer
		if err != nil {
			return err
		}
	}
	// throttle time ms for now this is just 0 as an int32
	binary.Write(buf, binary.BigEndian, int32(0))
	binary.Write(buf, binary.BigEndian, int8(0))
	return nil
}
