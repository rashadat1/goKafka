package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ServerConfig struct {
	ClientPort int
	BrokerPort int
	BrokerId   int
	DataDir    string
	Peers      BrokerPeers
}
type BrokerPeers []string

type PeerConnMap struct {
	Mu    sync.Mutex
	Peers map[int32]net.Conn
}

func (bp *BrokerPeers) String() string {
	return strings.Join(*bp, ",")
}
func (bp *BrokerPeers) Set(s string) error {
	*bp = strings.Split(s, ",")
	return nil
}

type ApiData struct {
	ApiVersions []int16
	MinVersion  int16
	MaxVersion  int16
}

var allApiData map[int16]*ApiData // slice of int16 versions that are valid per api key

type TopicRecord struct {
	// name and version from topic record in metadata file
	Version int8
	Name    string
}
type PartitionRecord struct {
	// version, partitionId, leader, leader_epoch, partition_epoch, replica, insync, removing, adding, and directories arrays from partition records in metadata file
	Version               int8
	PartitionId           int32
	Leader                int32
	LeaderEpoch           int32
	PartitionEpoch        int32
	ReplicaArray          []int32
	InSyncReplicaArray    []int32
	RemovingReplicasArray []int32
	AddingReplicasArray   []int32
	DirectoriesArray      []uuid.UUID
}
type FeatureLevelRecord struct {
	Version      int8
	FeatureLevel int16
}
type FetchRequestData struct {
	TopicIds           []uuid.UUID
	Partitions         map[uuid.UUID][]PartitionRequestData
	ForgottenTopicData map[uuid.UUID][]int32
}
type ProduceRequestData struct {
	TopicNames []string
	Partitions map[string][]ProducePartitionData
}
type ProducePartitionData struct {
	PartitionId int32
	Error       int16
}
type PartitionRequestData struct {
	PartitionId        int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

var TopicRecords map[uuid.UUID]TopicRecord
var PartitionRecords map[uuid.UUID][]PartitionRecord
var TopicNameToUUID map[string]uuid.UUID
var FeatureLevelRecords map[string]FeatureLevelRecord
var Configuration ServerConfig
var PeerConns PeerConnMap

type ByteReader struct {
	r *bytes.Reader
}

func (br *ByteReader) ReadInt8() (int8, error) {
	var res int8
	err := binary.Read(br.r, binary.BigEndian, &res)
	return res, err
}
func (br *ByteReader) ReadInt16() (int16, error) {
	var res int16
	err := binary.Read(br.r, binary.BigEndian, &res)
	return res, err
}
func (br *ByteReader) ReadInt32() (int32, error) {
	var res int32
	err := binary.Read(br.r, binary.BigEndian, &res)
	return res, err
}
func (br *ByteReader) ReadInt64() (int64, error) {
	var res int64
	err := binary.Read(br.r, binary.BigEndian, &res)
	return res, err
}
func (br *ByteReader) ReadUvarint() (uint64, error) {
	// unsigned variable-size integer
	return binary.ReadUvarint(br.r)
}
func (br *ByteReader) ReadVarint() (int64, error) {
	// signed variable-size integer
	return binary.ReadVarint(br.r)
}
func (br *ByteReader) ReadCompactString() (string, error) {
	// first read the length of the string
	length, err := br.ReadUvarint()
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	strBytes := make([]byte, length-1)
	_, err = io.ReadFull(br.r, strBytes)
	log.Printf("ReadCompactString: read '%s'", string(strBytes))
	return string(strBytes), err
}
func (br *ByteReader) ReadNBytesString(n int16) (string, error) {
	strBytes := make([]byte, n)
	_, err := io.ReadFull(br.r, strBytes)
	return string(strBytes), err
}
func (br *ByteReader) ReadNBytes(n int64) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := io.ReadFull(br.r, bytes)
	return bytes, err
}
func (br *ByteReader) ReadNullableString() (string, error) {
	// first read length of string
	length, err := br.ReadInt16()
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	strBytes := make([]byte, length)
	_, err = io.ReadFull(br.r, strBytes)
	log.Printf("ReadNullableString: read '%s'", string(strBytes))
	return string(strBytes), err
}
func (br *ByteReader) ReadCompactArray() ([]int32, error) {
	length, err := br.ReadUvarint()
	if err != nil {
		return nil, err
	}
	actual_length := int(length) - 1
	if actual_length == -1 {
		return nil, nil
	}
	compactArray := make([]int32, 0, actual_length)
	for i := 0; i < actual_length; i++ {
		val, _ := br.ReadInt32()
		compactArray = append(compactArray, val)
	}
	return compactArray, nil
}
func (br *ByteReader) ReadCompactUUIDArray() ([]uuid.UUID, error) {
	length, err := br.ReadUvarint()
	if err != nil {
		return nil, err
	}
	actual_length := int(length) - 1
	if actual_length == -1 {
		return nil, nil
	}
	compactArray := make([]uuid.UUID, 0, actual_length)
	for i := 0; i < actual_length; i++ {
		uuidBytes, _ := br.ReadNBytes(16)
		uuidVal, _ := uuid.FromBytes(uuidBytes)
		compactArray = append(compactArray, uuidVal)
	}
	return compactArray, nil
}
func fileExists(pathName string) bool {
	_, err := os.Stat(pathName)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true // only are checking if the file exists other errors we ignore in this function
}
func initializeApiData(allApiData map[int16]*ApiData) {
	for _, apiData := range allApiData {
		validVersions := apiData.ApiVersions
		for i := apiData.MinVersion; i <= apiData.MaxVersion; i++ {
			validVersions = append(validVersions, int16(i))
		}
		apiData.ApiVersions = validVersions
	}
}
func main() {
	allApiData = make(map[int16]*ApiData)
	TopicRecords = make(map[uuid.UUID]TopicRecord)
	PartitionRecords = make(map[uuid.UUID][]PartitionRecord)
	TopicNameToUUID = make(map[string]uuid.UUID)
	FeatureLevelRecords = make(map[string]FeatureLevelRecord)
	allApiData[int16(18)] = &ApiData{
		ApiVersions: make([]int16, 0),
		MinVersion:  int16(0),
		MaxVersion:  int16(4),
	}
	allApiData[int16(75)] = &ApiData{
		ApiVersions: make([]int16, 0),
		MinVersion:  int16(0),
		MaxVersion:  int16(0),
	}
	allApiData[int16(1)] = &ApiData{
		ApiVersions: make([]int16, 0),
		MinVersion:  int16(4),
		MaxVersion:  int16(16),
	}
	allApiData[int16(0)] = &ApiData{
		ApiVersions: make([]int16, 0),
		MinVersion:  int16(0),
		MaxVersion:  int16(11),
	}
	initializeApiData(allApiData)

	clientPort := flag.Int("client-port", 9092, "port number for server to listen on")
	brokerId := flag.Int("broker-id", 0, "id for the broker for identification")
	dataDir := flag.String("data-dir", "", "kraft combined logs location")
	brokerPort := flag.Int("broker-port", 9095, "port number for server to listen on for other brokers")

	var brokerPeers BrokerPeers
	flag.Var(&brokerPeers, "peers", "Comma-separated list of strings - locations of broker peers")

	flag.Parse()
	Configuration.ClientPort = *clientPort
	Configuration.BrokerId = *brokerId
	Configuration.DataDir = *dataDir
	Configuration.BrokerPort = *brokerPort
	Configuration.Peers = brokerPeers

	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", Configuration.ClientPort))
	if err != nil {
		log.Fatalf("Error initiating tcp server listening on port %d for clients: %v\n", Configuration.ClientPort, err)
	}
	defer ln.Close()
	log.Printf("Successfully created Kafka broker %d listening on port %d for clients...\n", Configuration.BrokerId, Configuration.ClientPort)

	lnServe, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", Configuration.BrokerPort))
	if err != nil {
		log.Fatalf("Error initiating tcp server listening on port %d for other brokers: %v\n", Configuration.BrokerPort, err)
	}
	defer lnServe.Close()
	log.Printf("Successfully created Kafka broker %d listening on port %d for brokers...\n", Configuration.BrokerId, Configuration.BrokerPort)

	if Configuration.DataDir == "" {
		if fileExists("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log") {
			file, err := os.OpenFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", os.O_RDONLY, 0644)
			if err != nil {
				log.Fatalf("Error opening metadata file: %v\n", err)
			}
			defer file.Close()
			log.Println("Opened metadata cluster file for reading")
			fileBytes, _ := io.ReadAll(file)
			fileReader := ByteReader{
				r: bytes.NewReader(fileBytes),
			}
			readClusterMetadataLog(&fileReader)
		}
	}
	clusterMetadataPath := fmt.Sprintf("%s/kraft-combined-logs/__cluster_metadata-0/", Configuration.DataDir)
	os.MkdirAll(clusterMetadataPath, 0777)

	log.Printf("Kafka broker peers: %v\n", Configuration.Peers)

	err = GenerateClusterMetadata(clusterMetadataPath, 2, Configuration.BrokerId)
	if err != nil {
		log.Fatalf("Error initializing cluster metadata file on broker %d: %v\n", Configuration.BrokerId, err)
	}

	// if we are running multiple brokers / this is not a codecrafters run
	err = os.MkdirAll(clusterMetadataPath, 0750) // create all intermediate directories
	if err != nil {
		log.Fatalf("Error initializing kraft combined logs directory: %v\n", err)
	}

	PeerConns.Peers = make(map[int32]net.Conn)
	wg := sync.WaitGroup{}
	wg.Add(len(Configuration.Peers)) // outbound dials
	wg.Add(len(Configuration.Peers)) // Inbound accepts
	go handleBroker(lnServe, &wg)
	for _, peer := range Configuration.Peers {
		go handleDial(peer, &wg)
	}
	wg.Wait()
	// this should be blocking because we do not want to accept client conns until each broker is configured properly
	requestMetadata() // each broker besides controller will send request metadata to controller
	wg.Add(len(Configuration.Peers))
	for brokerId, connection := range PeerConns.Peers {
		go sendMetadata(brokerId, connection, &wg)
	}
	wg.Wait()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("Error accepting connection made to server: %v\n", err)
			continue
		}
		log.Printf("Accepted new client connection from %v\n", conn.RemoteAddr())
		go handleConnection(conn)
	}
}
func sendMetadata(brokerId int32, conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	clusterMetadataFilePath := fmt.Sprintf("%s/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", Configuration.DataDir)
	if Configuration.BrokerId != 0 {
		return
	}
	log.Printf("Handling send metadata for broker: %d\n", brokerId)
	hasMetadataBytes := make([]byte, 1)
	var hasMetadataFlag int8

	conn.Read(hasMetadataBytes)
	binary.Read(bytes.NewReader(hasMetadataBytes), binary.BigEndian, &hasMetadataFlag)

	if hasMetadataFlag == int8(0) {
		log.Printf("Broker %d does not have the cluster metadata file\n", brokerId)
		metadataFile, err := os.OpenFile(clusterMetadataFilePath, os.O_RDONLY, 0777)
		if err != nil {
			log.Printf("Error opening cluster metadata file: %v\n", err)
			return
		}
		metadataFileBytes, err := io.ReadAll(metadataFile)
		if err != nil {
			log.Printf("Error reading cluster metadata file: %v\n", err)
		}
		metadataFileLength := int32(len(metadataFileBytes))
		buf := new(bytes.Buffer)

		binary.Write(buf, binary.BigEndian, metadataFileLength)
		conn.Write(buf.Bytes()) // write cluster metadata length
		// now write the whole file
		log.Printf("Sent cluster metadata file length as num bytes (int32): %d\n", metadataFileLength)
		n, err := conn.Write(metadataFileBytes)
		if err != nil {
			log.Printf("Error writing cluster metadata from broker 0 to peer: %d\n", brokerId)
		}
		log.Printf("Wrote %d bytes to peer %d\n", n, brokerId)
	}

}
func requestMetadata() {
	clusterMetadataFilePath := fmt.Sprintf("%s/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", Configuration.DataDir)
	if Configuration.BrokerId == 0 {
		return
	}
	controllerCon := PeerConns.Peers[int32(0)]
	var has_metadata int8
	if fileExists(clusterMetadataFilePath) {
		has_metadata = int8(1)
		log.Printf("Cluster Metadata file already exists in expected location for %d\n", Configuration.BrokerId)
	} else {
		log.Printf("Broker with id %d does not have the Cluster Metadata file\n", Configuration.BrokerId)
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, has_metadata)
	log.Println("Writing has_metadata flag to controller broker")
	controllerCon.Write(buf.Bytes())
	if has_metadata == int8(0) {
		// controller will respond with the length of the cluster metadata file as int32
		// and then the contents of the file themselves
		var clusterMetadataFileLength int32
		fileLengthBytes := make([]byte, 4)
		controllerCon.Read(fileLengthBytes)
		binary.Read(bytes.NewReader(fileLengthBytes), binary.BigEndian, &clusterMetadataFileLength)

		metadataFileBytes := make([]byte, clusterMetadataFileLength)
		bytesRead := 0
		for {
			if int32(bytesRead) == clusterMetadataFileLength {
				break
			}
			n, err := controllerCon.Read(metadataFileBytes)
			if err != nil {
				if err == io.EOF {
					log.Println("Client closed connection")
					return
				}
				log.Printf("Faled to read bytes from channel while reading cluster metadata file: %v", err)
				return
			}
			bytesRead += n
		}
		metadataFile, err := os.OpenFile(clusterMetadataFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0777)
		if err != nil {
			log.Printf("Error creating or opening cluster metadata file: %v", err)
			return
		}
		defer metadataFile.Close()
		metadataFile.Write(metadataFileBytes)
	}
}
func handleBroker(lnServe net.Listener, wg *sync.WaitGroup) {
	acceptedConnectionCount := 0
	expectedConnectionNum := len(Configuration.Peers)
	for expectedConnectionNum > acceptedConnectionCount {
		conn, err := lnServe.Accept()
		if err != nil {
			log.Fatalf("Error acception connection made to server from broker: %v\n", err)
			continue
		}
		go handleInitHandshake(conn, wg)
		acceptedConnectionCount++
	}
}
func handleInitHandshake(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Accepted new broker connection from %v\n", conn.RemoteAddr())
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int32(Configuration.BrokerId))
	conn.Write(buf.Bytes())

	brokerIdPeerBytes := make([]byte, 4)
	conn.Read(brokerIdPeerBytes)

	var brokerIdPeer int32
	binary.Read(bytes.NewReader(brokerIdPeerBytes), binary.BigEndian, &brokerIdPeer)

	if brokerIdPeer < int32(Configuration.BrokerId) {
		PeerConns.Mu.Lock()
		PeerConns.Peers[brokerIdPeer] = conn
		PeerConns.Mu.Unlock()
	} else {
		conn.Close()
	}
}
func handleDial(peerAddr string, wg *sync.WaitGroup) {
	defer wg.Done()
	retry := 0
	maxRetry := 5
	for {
		if retry == maxRetry {
			return
		}
		conn, err := net.Dial("tcp", peerAddr)
		if err == nil {
			log.Printf("Established connection to peer at address: %s\n", peerAddr)
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, int32(Configuration.BrokerId))
			conn.Write(buf.Bytes())

			brokerIdPeerBytes := make([]byte, 4)
			conn.Read(brokerIdPeerBytes)

			var brokerIdPeer int32
			binary.Read(bytes.NewReader(brokerIdPeerBytes), binary.BigEndian, &brokerIdPeer)

			if brokerIdPeer < int32(Configuration.BrokerId) {
				// if the peers broker id is smaller than this server's
				conn.Close()

			} else {
				PeerConns.Mu.Lock()
				PeerConns.Peers[brokerIdPeer] = conn
				PeerConns.Mu.Unlock()
			}
			return
		}
		sleepTime := math.Pow(4, float64(retry))
		log.Printf("Failed to establish connection with peer at address: %s, retrying in %v...\n", peerAddr, sleepTime)
		time.Sleep(time.Duration(sleepTime) * time.Second)
		retry++
	}
}
func handleConnection(conn net.Conn) {
	correlation_id := int32(7)
	msg_size := int32(0)
	for {
		error_code := int16(0)
		msgSizeBytes := make([]byte, 4)
		_, err := conn.Read(msgSizeBytes)
		log.Printf("MsgSizeBytes: %v\n", msgSizeBytes)
		if err != nil {
			if err == io.EOF {
				log.Println("Client closed connection")
				return
			}
			log.Printf("Error reading bytes from client: %v\n", err)
			return
		}

		msg_size, err = getInt32FromBytes(msgSizeBytes)
		if err != nil {
			log.Printf("Failed parsing the message size from the client request: %v\n", err)
			continue
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
				if err == io.EOF {
					log.Println("Client closed connection")
					return
				}
				log.Printf("Failed to read bytes from channel: %v\n", err)
				return
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
		log.Printf("Full Bytes after message size: %v\n", fullBody)
		// read expected number of bytes without issue
		// first read the request api key and api version
		br := ByteReader{
			r: bytes.NewReader(fullBody),
		}
		request_api_key, _ := br.ReadInt16()
		request_api_version, _ := br.ReadInt16()
		// for now we will not validate the api key
		if !isValidApiVersion(request_api_key, request_api_version) {
			error_code = int16(35)
		}
		// get correlation_id which is 4 bytes starting at the 9th
		correlation_id, err := br.ReadInt32()
		if err != nil {
			log.Printf("Failed parsing correlation id, request id, or api key bytes from the client request: %v\n", err)
		}
		log.Printf("Correlation id int32: %v\n", correlation_id)

		bodyBuf := new(bytes.Buffer)
		err = binary.Write(bodyBuf, binary.BigEndian, correlation_id)
		if err != nil {
			log.Printf("Failed to write correlation id to buffer: %v\n", err)
		}
		switch request_api_key {
		case int16(18):
			err = binary.Write(bodyBuf, binary.BigEndian, error_code)
			if err != nil {
				log.Printf("Failed to write error code to buffer: %v\n", err)
			}
			if error_code == int16(0) {
				err = constructApiVersionResponse(bodyBuf, allApiData)
				if err != nil {
					log.Printf("Failed to write api version bytes to buffer: %v\n", err)
				}
			}
		case int16(75):
			constructDescribeTopicPartitionsResponse(bodyBuf, &br)
		case int16(1):
			binary.Write(bodyBuf, binary.BigEndian, int8(0)) // tag buffer after correlation id
			constructFetchResponse(bodyBuf, &br)
		case int16(0):
			binary.Write(bodyBuf, binary.BigEndian, int8(0)) // tag buffer after correlation id
			constructProduceResponse(bodyBuf, &br)
		default:
			log.Printf("Api Key case not implemented yet: %v", request_api_key)
		}
		response_size := int32(len(bodyBuf.Bytes()))
		fullBuf := new(bytes.Buffer)
		binary.Write(fullBuf, binary.BigEndian, response_size)
		fullBuf.Write(bodyBuf.Bytes())
		log.Printf("Bytes in buffer: %v\n", fullBuf.Bytes())
		conn.Write(fullBuf.Bytes())
	}
}
func getInt32FromBytes(byteSlice []byte) (int32, error) {
	var res int32
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
func constructApiVersionResponse(buf *bytes.Buffer, allApiData map[int16]*ApiData) error {
	num_api_keys := (len(allApiData) + 1)
	writeUvarintField(uint64(num_api_keys), buf)
	for apiKey, value := range allApiData {
		err := binary.Write(buf, binary.BigEndian, apiKey)
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
func constructDescribeTopicPartitionsResponse(buf *bytes.Buffer, br *ByteReader) {
	// tag buffer
	binary.Write(buf, binary.BigEndian, int8(0))
	// duration in milliseconds for which request was throttled due to quota violation
	throttle_time := int32(0)
	binary.Write(buf, binary.BigEndian, throttle_time)
	topicArray := getDescribeTopicPartitionTopicNames(br)

	writeUvarintField(uint64(len(topicArray)+1), buf)
	for i := 0; i < len(topicArray); i++ {
		topicUUID := uuid.Nil
		errorCode := int16(3)
		topicName := topicArray[i]
		uuidInMap, ok := TopicNameToUUID[topicName]
		if ok {
			// topic exists
			log.Printf("Topic exists in map with uuid: %v\n", uuidInMap)
			topicUUID = uuidInMap
			errorCode = int16(0)
		}
		binary.Write(buf, binary.BigEndian, errorCode)
		log.Printf("Topic name to write next: %s\n", topicName)
		topicNameBytes := []byte(topicName)
		writeUvarintField(uint64(len(topicNameBytes)+1), buf)
		binary.Write(buf, binary.BigEndian, topicNameBytes)
		binary.Write(buf, binary.BigEndian, []byte(topicUUID[:]))
		binary.Write(buf, binary.BigEndian, int8(0)) // is internal
		// compact partitions array - right now it is empty
		// need to loop through the partitions of this topic
		partitions, ok := PartitionRecords[topicUUID]
		if !ok {
			// no partitions in this case
			// but is is a compact_array of partitions for the topic containing the length + 1 encoded as unsigned varint and then the contents
			writeUvarintField(uint64(0+1), buf)
		}
		if ok {
			writeUvarintField(uint64(len(partitions)+1), buf)
			for _, partition := range partitions {
				binary.Write(buf, binary.BigEndian, int16(0))              // error code 0 so no error
				binary.Write(buf, binary.BigEndian, partition.PartitionId) // partition index / id
				binary.Write(buf, binary.BigEndian, partition.Leader)      // leader id
				binary.Write(buf, binary.BigEndian, partition.LeaderEpoch)
				writeUvarintField(uint64(len(partition.ReplicaArray)+1), buf)
				for _, replicaId := range partition.ReplicaArray {
					binary.Write(buf, binary.BigEndian, replicaId)
				}
				writeUvarintField(uint64(len(partition.InSyncReplicaArray)+1), buf)
				for _, isrId := range partition.InSyncReplicaArray {
					binary.Write(buf, binary.BigEndian, isrId)
				}
				// elligible leader replicas
				writeUvarintField(uint64(0+1), buf)          // elligible leader replicas
				writeUvarintField(uint64(0+1), buf)          // last known elligible leader replicas
				writeUvarintField(uint64(0+1), buf)          // offline replicas
				binary.Write(buf, binary.BigEndian, int8(0)) // tag buffer

			}
		}
		// here we write the topic authorized operations which should also be a method that reads from somewhere
		// probably a file a 4 byte big endian integer of 0 or 1 bits for each "index" corresponding to a particular operation
		binary.Write(buf, binary.BigEndian, defaultTopicPermissions())
		binary.Write(buf, binary.BigEndian, int8(0)) // tag buffer
	}

	binary.Write(buf, binary.BigEndian, int8(-1)) // next cursor
	binary.Write(buf, binary.BigEndian, int8(0))  // tag buffer
}
func getDescribeTopicPartitionTopicNames(br *ByteReader) []string {
	topicNames := make([]string, 0)
	clientId, _ := br.ReadNullableString() // variable length client id
	log.Printf("Client ID: %s\n", clientId)
	br.ReadInt8() // tag buffer field
	arrayLengthVal, _ := br.ReadUvarint()
	arrayLength := arrayLengthVal - 1
	log.Printf("Array of Topic Names in describe topic partitions request is of length: %v\n", arrayLength)
	for i := 0; i < int(arrayLength); i++ {
		topicName, _ := br.ReadCompactString()
		log.Printf("Topic #%d: %s\n", i, topicName)
		topicNames = append(topicNames, topicName)
		br.ReadInt8() // read tag buffer byte
	}
	log.Printf("Returning topics in order: %v\n", topicNames)
	return topicNames
}
func parseFetchRequest(br *ByteReader) *FetchRequestData {
	// parse fetch request
	fdata := &FetchRequestData{
		TopicIds:           make([]uuid.UUID, 0),
		Partitions:         make(map[uuid.UUID][]PartitionRequestData),
		ForgottenTopicData: make(map[uuid.UUID][]int32),
	}
	br.ReadNullableString() // read client id length and then client id string
	br.ReadInt8()           // tag buffer after last header
	br.ReadInt32()          // max wait ms
	br.ReadInt32()          // min bytes
	br.ReadInt32()          // max bytes
	br.ReadInt8()           // isolation level
	br.ReadInt32()          // session id
	br.ReadInt32()          // session epoch
	// topic length as topics is a compact array
	topicArrayValue, _ := br.ReadUvarint()
	topicArrayLength := int(topicArrayValue) - 1
	log.Printf("Num topics referenced in fetch request: %d\n", topicArrayLength)
	for i := 0; i < int(topicArrayLength); i++ {
		topic_id, _ := br.ReadNBytes(16)
		topic_uuid, _ := uuid.FromBytes(topic_id)
		// read partition array length
		partitionArrayValue, _ := br.ReadUvarint()
		partitionArrayLength := int(partitionArrayValue) - 1
		log.Printf("Topic Id: %s has %d partitions in Fetch Request\n", topic_uuid.String(), partitionArrayLength)
		for j := 0; j < int(partitionArrayLength); j++ {
			partitionId, _ := br.ReadInt32()
			currLeaderEpoch, _ := br.ReadInt32()
			fetchOffset, _ := br.ReadInt64()
			lastFetchedEpoch, _ := br.ReadInt32()
			logStartOffset, _ := br.ReadInt64()
			partitonMaxBytes, _ := br.ReadInt32()

			br.ReadInt8() // empty tag buffer
			currPartition := PartitionRequestData{
				PartitionId:        partitionId,
				CurrentLeaderEpoch: currLeaderEpoch,
				FetchOffset:        fetchOffset,
				LastFetchedEpoch:   lastFetchedEpoch,
				LogStartOffset:     logStartOffset,
				PartitionMaxBytes:  partitonMaxBytes,
			}
			if _, ok := fdata.Partitions[topic_uuid]; ok {
				fdata.Partitions[topic_uuid] = append(fdata.Partitions[topic_uuid], currPartition)
			} else {
				fdata.Partitions[topic_uuid] = make([]PartitionRequestData, 0)
				fdata.Partitions[topic_uuid] = append(fdata.Partitions[topic_uuid], currPartition)
			}
		}
		br.ReadInt8() // empty tag buffer
		fdata.TopicIds = append(fdata.TopicIds, topic_uuid)
	}
	br.ReadInt8() // topic array tag buffer
	forgottenTopicValue, _ := br.ReadUvarint()
	forgottenTopicLength := int(forgottenTopicValue) - 1
	log.Printf("Forgotten Topic Data length in Fetch request is: %d\n", forgottenTopicLength)
	for i := 0; i < int(forgottenTopicLength); i++ {
		topic_id, _ := br.ReadNBytes(16)
		topic_uuid, _ := uuid.FromBytes(topic_id)
		fdata.ForgottenTopicData[topic_uuid] = make([]int32, 0)
		partitionIdArrayValue, _ := br.ReadUvarint()
		partitionIdArrayLength := int(partitionIdArrayValue) - 1
		for j := 0; j < int(partitionIdArrayLength); j++ {
			partitionId, _ := br.ReadInt32()
			fdata.ForgottenTopicData[topic_uuid] = append(fdata.ForgottenTopicData[topic_uuid], partitionId)
		}
		br.ReadInt8() // read last tagged buffer
	}
	return fdata

}
func constructFetchResponse(buf *bytes.Buffer, br *ByteReader) {
	fetchData := parseFetchRequest(br)
	binary.Write(buf, binary.BigEndian, int32(0)) // throttle time ms
	binary.Write(buf, binary.BigEndian, int16(0)) // error code
	binary.Write(buf, binary.BigEndian, int32(0)) // fetch session id or 0 if not part of fetch session
	requestTopics := fetchData.TopicIds
	writeUvarintField(uint64(len(requestTopics)+1), buf) // num responses + 1 = num of req topics + 1
	for i := 0; i < len(requestTopics); i++ {
		binary.Write(buf, binary.BigEndian, []byte(requestTopics[i][:]))
		requestTopicPartitions := fetchData.Partitions[requestTopics[i]]
		for _, partition := range requestTopicPartitions {
			writeUvarintField(uint64(len(requestTopicPartitions)+1), buf)
			if _, ok := TopicRecords[requestTopics[i]]; !ok {
				// topic does not exist
				binary.Write(buf, binary.BigEndian, int32(0))   // partition index
				binary.Write(buf, binary.BigEndian, int16(100)) // error code (non-existent topic)
				binary.Write(buf, binary.BigEndian, int64(-1))  // high watermark
				binary.Write(buf, binary.BigEndian, int64(-1))  // last stable offset
				binary.Write(buf, binary.BigEndian, int64(-1))  // log start offset
				writeUvarintField(uint64(1), buf)               // null aborted transactions
				binary.Write(buf, binary.BigEndian, int32(-1))  // no preferred read replicas
				writeUvarintField(uint64(0), buf)               // null compact array of records
				binary.Write(buf, binary.BigEndian, int8(0))    // write tag buffer per partition
			} else {
				// topic exists
				topicRecord := TopicRecords[requestTopics[i]]
				topicName := topicRecord.Name

				partitionNumber := partition.PartitionId
				logFilePath := fmt.Sprintf("%s/kraft-combined-logs/%s-%d/00000000000000000000.log", Configuration.DataDir, topicName, partitionNumber)
				if !fileExists(logFilePath) {
					binary.Write(buf, binary.BigEndian, int32(0))  // partition index
					binary.Write(buf, binary.BigEndian, int16(0))  // error code (no error)
					binary.Write(buf, binary.BigEndian, int64(-1)) // high watermark
					binary.Write(buf, binary.BigEndian, int64(-1)) // last stable offset
					binary.Write(buf, binary.BigEndian, int64(-1)) // log start offset
					writeUvarintField(uint64(1), buf)              // null aborted transactions
					binary.Write(buf, binary.BigEndian, int32(-1)) // no preferred read replicas
					writeUvarintField(uint64(1), buf)              // empty compact array of records
					binary.Write(buf, binary.BigEndian, int8(0))
				} else {
					partitionFile, _ := os.OpenFile(logFilePath, os.O_RDONLY, 0644)
					partitionFileBytes, err := io.ReadAll(partitionFile)
					if err != nil {
						log.Fatalf("failed to read partition file %s", logFilePath)
					}
					partitionFileReader := ByteReader{
						r: bytes.NewReader(partitionFileBytes),
					}
					recordBatches := readPartitionLog(&partitionFileReader, partition)
					binary.Write(buf, binary.BigEndian, int32(partition.PartitionId)) // partition index
					binary.Write(buf, binary.BigEndian, int16(0))                     // error code (no error)
					binary.Write(buf, binary.BigEndian, int64(-1))                    // high watermark
					binary.Write(buf, binary.BigEndian, int64(-1))                    // last stable offset
					binary.Write(buf, binary.BigEndian, int64(-1))                    // log start offset
					writeUvarintField(uint64(1), buf)                                 // null aborted transactions
					binary.Write(buf, binary.BigEndian, int32(-1))                    // no preferred read replicas
					writeUvarintField(uint64(len(recordBatches)+1), buf)              // write compact byte array
					buf.Write(recordBatches)
					binary.Write(buf, binary.BigEndian, int8(0)) // tag buffer per partition
				}

			}
		}
		binary.Write(buf, binary.BigEndian, int8(0)) // write tag buffer per topic
	}
	binary.Write(buf, binary.BigEndian, int8(0)) // tag buffer for whole message
}
func parseProduceRequest(br *ByteReader) *ProduceRequestData {
	prd := &ProduceRequestData{
		TopicNames: make([]string, 0),
		Partitions: make(map[string][]ProducePartitionData, 0),
	}
	br.ReadNullableString() // read client id length (int16) and then read <length> bytes
	br.ReadInt8()           // read tag buffer
	log.Println("Reading compact string transactional id...")
	br.ReadCompactString() // read transactional id
	log.Println("Finished reading compact string transactional id...")
	br.ReadInt16() // number of acknowledges the producer requires the leader to receive from replicas for request to be success
	br.ReadInt32() // timeout (to await a response)
	/*
		type ProduceRequestData struct {
			TopicNames   []string
			Partitions map[string][]ProducePartitionData
		}
		var TopicRecords map[uuid.UUID]TopicRecord
		var PartitionRecords map[uuid.UUID][]PartitionRecord
		var TopicNameToUUID map[string]uuid.UUID
		var FeatureLevelRecords map[string]FeatureLevelRecord

	*/
	topicArrayVal, _ := br.ReadUvarint()
	topicArrayLength := topicArrayVal - 1
	log.Printf("\ntopic array length: %v", topicArrayLength)
	for i := 0; i < int(topicArrayLength); i++ {
		topicName, _ := br.ReadCompactString()
		prd.TopicNames = append(prd.TopicNames, topicName)
		prd.Partitions[topicName] = make([]ProducePartitionData, 0)

		partitionArrayVal, _ := br.ReadUvarint()
		partitionArrayLength := partitionArrayVal - 1
		log.Printf("\npartition array length: %v", partitionArrayLength)

		for j := 0; j < int(partitionArrayLength); j++ {
			ppd := &ProducePartitionData{
				Error: int16(0),
			}
			partitionId, _ := br.ReadInt32()
			ppd.PartitionId = partitionId
			// for each topic save data for each partition of the topic that is requested
			// check for each topic Name and partitionId if it exists
			topicUUID, ok1 := TopicNameToUUID[topicName]
			if !ok1 {
				// topic does not exist
				log.Printf("Topic does not exist: %s\n", topicUUID.String())
				ppd.Error = int16(3) // unknown topic or partition error
			} else {
				partitionRecords := PartitionRecords[topicUUID]
				foundPartition := false
				for _, partitionRecord := range partitionRecords {
					if partitionRecord.PartitionId == partitionId {
						foundPartition = true
						break
					}
				}
				if !foundPartition {
					log.Printf("Partition does not exist under this topic: %s\n", topicUUID.String())
					// partition does not exist under this topic
					ppd.Error = int16(3) // unknown topic or partition error
				}
			}
			prd.Partitions[topicName] = append(prd.Partitions[topicName], *ppd)
			recordBatchSizeVal, _ := br.ReadUvarint()
			recordBatchBytes := make([]byte, recordBatchSizeVal-1)

			_, err := io.ReadFull(br.r, recordBatchBytes)
			if err != nil {
				log.Printf("Error reading record batch from produce request: %v\n", err)
			}
			partitionLogFile := fmt.Sprintf("%s/kraft-combined-logs/%s-%d/00000000000000000000.log", Configuration.DataDir, topicName, partitionId)
			partitionFile, _ := os.OpenFile(partitionLogFile, os.O_APPEND|os.O_WRONLY, 0777)

			_, err = partitionFile.Write(recordBatchBytes)
			if err != nil {
				log.Printf("Error writing record batch to log file: %v\n", err)
			}
			br.ReadInt8() // empty tag buffer for partition
		}
		br.ReadInt8() // empty tag buffer for topic
	}
	br.ReadInt8() // empty tag buffer for request
	return prd
}

func constructProduceResponse(buf *bytes.Buffer, br *ByteReader) {
	prd := parseProduceRequest(br)
	writeUvarintField(uint64(len(prd.TopicNames)+1), buf) // write length of topic array
	for i := 0; i < len(prd.TopicNames); i++ {
		writeUvarintField(uint64(len(prd.TopicNames[i])+1), buf) // write topic name string length
		binary.Write(buf, binary.BigEndian, []byte(prd.TopicNames[i]))

		allPartitions := prd.Partitions[prd.TopicNames[i]]
		writeUvarintField(uint64(len(allPartitions)+1), buf)
		for j := 0; j < len(allPartitions); j++ {
			binary.Write(buf, binary.BigEndian, allPartitions[j].PartitionId) // write partition id
			binary.Write(buf, binary.BigEndian, allPartitions[j].Error)       // write error code
			if allPartitions[j].Error != int16(0) {
				// fields -1 on error
				binary.Write(buf, binary.BigEndian, int64(-1)) // base offset
				binary.Write(buf, binary.BigEndian, int64(-1)) // log append time
				binary.Write(buf, binary.BigEndian, int64(-1)) // log start offset
				binary.Write(buf, binary.BigEndian, int8(1))   // record error array length
				binary.Write(buf, binary.BigEndian, int8(0))   // record error array error messages
			} else {
				binary.Write(buf, binary.BigEndian, int64(0))  // base offset
				binary.Write(buf, binary.BigEndian, int64(-1)) // log append time
				binary.Write(buf, binary.BigEndian, int64(0))  // log start offset
				binary.Write(buf, binary.BigEndian, int8(1))   // record error array length
				binary.Write(buf, binary.BigEndian, int8(0))   // record error array error messages
			}
			binary.Write(buf, binary.BigEndian, int8(0)) // tag buffer on partition
		}
		binary.Write(buf, binary.BigEndian, int8(0)) // tag buffer on topic
	}
	binary.Write(buf, binary.BigEndian, int32(0)) // throttle time
	binary.Write(buf, binary.BigEndian, int8(0))  // tag buffer on response

}
func writeUvarintField(num uint64, buf *bytes.Buffer) {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, num)
	buf.Write(tmp[:n]) // only write the number of bytes we actually used out of the maximum, this constant MaxVarintLen64 = 10
}
func defaultTopicPermissions() uint32 {
	// bit-shift or assignment. authorizedOps starts out as all 0s so this assigns 1 at the nth position where (1 << n) in the bitfield
	var authorizedOps uint32
	authorizedOps |= (1 << 3)
	authorizedOps |= (1 << 4)
	authorizedOps |= (1 << 5)
	authorizedOps |= (1 << 6)
	authorizedOps |= (1 << 7)
	authorizedOps |= (1 << 8)
	authorizedOps |= (1 << 10)
	authorizedOps |= (1 << 11)

	return authorizedOps
}
func readUvarintField(requestBytes []byte) uint64 {
	r := bytes.NewReader(requestBytes)
	val, _ := binary.ReadUvarint(r)
	return val
}
func readClusterMetadataLog(br *ByteReader) {
	for {
		if br.r.Len() == 0 {
			break
		}
		br.ReadInt64()                   // base offset
		br.ReadInt32()                   // batch length
		br.ReadInt32()                   // partition leader epoch
		br.ReadInt8()                    // magic byte
		br.ReadInt32()                   // CRC
		br.ReadInt16()                   // attributes
		br.ReadInt32()                   // last offset delta
		br.ReadInt64()                   // base timestamp
		br.ReadInt64()                   // mmax timestamp
		br.ReadInt64()                   // producerId
		br.ReadInt16()                   // producer epoch
		br.ReadInt32()                   // base sequence number
		num_records, _ := br.ReadInt32() // number of records in the batch
		log.Printf("Number of records in the batch: %d\n", num_records)
		for i := 0; int32(i) < num_records; i++ {
			// now finally we can loop through the records
			br.ReadVarint()                  // length of the record
			br.ReadInt8()                    // 1 byte attribute not used in protocol lol
			br.ReadVarint()                  // difference between base timestamp of batch and that of the record
			br.ReadVarint()                  // offset delta
			key_length, _ := br.ReadVarint() // key length
			if key_length != int64(-1) {
				// key is not null
				log.Printf("Key in Cluster Metadata log file is not null, has length: %d\n", key_length)
				log.Printf("For records #%d\n", i)
				br.ReadNBytes(key_length)
			}
			br.ReadVarint() // value length
			// now we have reached the value - the actual record data for topic or partition
			// the first byte is the frame version which we do not care about at least for now
			br.ReadInt8()                   // frame version
			record_type, _ := br.ReadInt8() // type of the record
			log.Printf("Record Type in metadata log is: %d\n", record_type)
			switch record_type {
			case 2:
				log.Printf("Found topic record in cluster metadata log file")
				// topic record
				version, _ := br.ReadInt8()
				topic_name, _ := br.ReadCompactString()
				uuidBytes, _ := br.ReadNBytes(16)
				val_uuid, _ := uuid.FromBytes(uuidBytes)
				tagFieldCount, _ := br.ReadUvarint()
				if tagFieldCount != 0 {
					log.Printf("Tagged Fields Count is %d, only handle 0 currently\n", tagFieldCount)
				}
				tr := TopicRecord{
					Name:    topic_name,
					Version: version,
				}
				TopicRecords[val_uuid] = tr
				TopicNameToUUID[topic_name] = val_uuid
			case 3:
				log.Printf("Found partition record in cluster metadata log file")
				version, _ := br.ReadInt8()
				partition_id, _ := br.ReadInt32()
				uuidBytes, _ := br.ReadNBytes(16)
				val_uuid, _ := uuid.FromBytes(uuidBytes)
				replicaArray, _ := br.ReadCompactArray()
				inSyncReplicasArray, _ := br.ReadCompactArray()
				removingReplicasArray, _ := br.ReadCompactArray()
				addingReplicasArray, _ := br.ReadCompactArray()
				leaderReplicaId, _ := br.ReadInt32()
				leaderEpoch, _ := br.ReadInt32()
				partitionEpoch, _ := br.ReadInt32()
				directoriesArray, _ := br.ReadCompactUUIDArray()
				tagFieldCount, _ := br.ReadUvarint()
				if tagFieldCount != 0 {
					log.Printf("Tagged Fields Count is %d, only handle 0 currently\n", tagFieldCount)
				}
				pr := PartitionRecord{
					Version:               version,
					PartitionId:           partition_id,
					Leader:                leaderReplicaId,
					LeaderEpoch:           leaderEpoch,
					PartitionEpoch:        partitionEpoch,
					ReplicaArray:          replicaArray,
					InSyncReplicaArray:    inSyncReplicasArray,
					RemovingReplicasArray: removingReplicasArray,
					AddingReplicasArray:   addingReplicasArray,
					DirectoriesArray:      directoriesArray,
				}
				if _, ok := PartitionRecords[val_uuid]; ok {
					PartitionRecords[val_uuid] = append(PartitionRecords[val_uuid], pr)
				} else {
					PartitionRecords[val_uuid] = make([]PartitionRecord, 0)
					PartitionRecords[val_uuid] = append(PartitionRecords[val_uuid], pr)
				}
			case 12:
				log.Printf("Found feature level record")
				version, _ := br.ReadInt8()
				name, _ := br.ReadCompactString()
				featureLevel, _ := br.ReadInt16()
				tagFieldCount, _ := br.ReadUvarint()
				if tagFieldCount != 0 {
					log.Printf("Tagged Fields Count is %d, only handle 0 currently\n", tagFieldCount)
				}
				flr := FeatureLevelRecord{
					Version:      version,
					FeatureLevel: featureLevel,
				}
				FeatureLevelRecords[name] = flr
			default:
				log.Printf("Not implemented yet")
			}
			br.ReadCompactArray() // header array
		}
	}
}
func readPartitionLog(br *ByteReader, PartitionRequestData PartitionRequestData) []byte {
	fetchRequestOffset := PartitionRequestData.FetchOffset
	for {
		if br.r.Len() == 0 {
			break
		}
		batchBaseOffset, _ := br.ReadInt64()
		batchLength, _ := br.ReadInt32()
		br.ReadInt32()                        // partition leader epoch
		br.ReadInt8()                         // magic byte
		br.ReadInt32()                        // checksum record batch
		br.ReadInt16()                        // attributes
		batchOffsetDelta, _ := br.ReadInt32() // last offset delta

		if batchBaseOffset+int64(batchOffsetDelta) < fetchRequestOffset {
			br.r.Seek(int64(batchLength)-15, io.SeekCurrent) // seek to end of batch - we read 15 bytes into
		} else if (batchBaseOffset <= fetchRequestOffset) && (batchBaseOffset+int64(batchOffsetDelta) >= fetchRequestOffset) {
			br.r.Seek(-27, io.SeekCurrent)
			recordBatches := make([]byte, min(PartitionRequestData.PartitionMaxBytes, int32(br.r.Len())))
			_, err := br.r.Read(recordBatches)
			if err != nil {
				log.Fatalf("Error reading record batch bytes into byte buffer for fetch request: %v", err)
			}
			return recordBatches
		} else {
			// batch base offset > fetch request offset
			break
		}

	}
	return nil
}
