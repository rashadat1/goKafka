package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
)

type RecordBatch struct {
	PartitionLeaderEpoch int32
	MagicByte            int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordsLength        int32
	Records              []*Record
}

func (rb *RecordBatch) Encode(batchNum int64) []byte {
	wholeBuf := new(bytes.Buffer)
	bodyBuf := new(bytes.Buffer)
	checkSumBuf := new(bytes.Buffer)
	binary.Write(bodyBuf, binary.BigEndian, rb.PartitionLeaderEpoch)
	binary.Write(bodyBuf, binary.BigEndian, rb.MagicByte)
	// fields to calculate checksum over
	binary.Write(checkSumBuf, binary.BigEndian, rb.Attributes)
	binary.Write(checkSumBuf, binary.BigEndian, rb.LastOffsetDelta)
	binary.Write(checkSumBuf, binary.BigEndian, rb.BaseTimestamp)
	binary.Write(checkSumBuf, binary.BigEndian, rb.MaxTimestamp)
	binary.Write(checkSumBuf, binary.BigEndian, rb.ProducerId)
	binary.Write(checkSumBuf, binary.BigEndian, rb.ProducerEpoch)
	binary.Write(checkSumBuf, binary.BigEndian, rb.BaseSequence)
	binary.Write(checkSumBuf, binary.BigEndian, rb.RecordsLength)
	for _, record := range rb.Records {
		recordVal := *record
		checkSumBuf.Write(recordVal.Encode())
	}
	// calculate checksum of all of the above data and write to bodyBuf
	crcTable := crc32.MakeTable(crc32.Castagnoli)
	checkSum := crc32.Checksum(checkSumBuf.Bytes(), crcTable)
	checkSumVal := int32(checkSum)

	binary.Write(bodyBuf, binary.BigEndian, checkSumVal)
	bodyBuf.Write(checkSumBuf.Bytes())
	batchLength := bodyBuf.Len()
	// write the base offset and batchlength to the final buffer and then append all other
	// bytes from bodyBuf
	binary.Write(wholeBuf, binary.BigEndian, batchNum)
	binary.Write(wholeBuf, binary.BigEndian, int32(batchLength))
	wholeBuf.Write(bodyBuf.Bytes())
	return wholeBuf.Bytes()
}

type Record struct {
	Length            int64 // variable-size signed integer
	Attributes        int8
	TimestampDelta    int64
	OffsetDelta       int64
	KeyLength         int64  // just make this int64(-1)
	Key               string // nullable string written as byte array has length Key Length
	ValueLength       int64
	Value             RecordData
	HeadersArrayCount uint64
}

func (r *Record) Encode() []byte {
	buf := new(bytes.Buffer)
	wholeBuf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.Attributes)
	binary.Write(buf, binary.BigEndian, r.TimestampDelta)
	binary.Write(buf, binary.BigEndian, r.OffsetDelta)
	binary.Write(buf, binary.BigEndian, r.KeyLength)
	buf.Write([]byte(r.Key))

	recordValBytes := r.Value.Encode()
	writeUvarintField(uint64(len(recordValBytes)+1), buf)
	binary.Write(buf, binary.BigEndian, len(recordValBytes))
	buf.Write(recordValBytes)
	writeUvarintField(r.HeadersArrayCount, buf)

	binary.Write(wholeBuf, binary.BigEndian, buf.Len())
	wholeBuf.Write(buf.Bytes())
	return wholeBuf.Bytes()
}

type RecordData interface {
	Encode() []byte
	RecordType() int8
}
type TopicRecordWrite struct {
	FrameVersion      int8
	Type              int8
	Version           int8
	TopicName         string
	TopicUUID         uuid.UUID
	TaggedFieldsCount uint64
}

func (trw *TopicRecordWrite) RecordType() int8 {
	return int8(2)
}
func (trw *TopicRecordWrite) Encode() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, trw.FrameVersion)
	binary.Write(buf, binary.BigEndian, trw.Type)
	binary.Write(buf, binary.BigEndian, trw.Version)

	topicNameBytes := []byte(trw.TopicName)
	topicNameLength := uint64(len(topicNameBytes))

	writeUvarintField(topicNameLength+1, buf)
	buf.Write(topicNameBytes)
	buf.Write(trw.TopicUUID[:])

	writeUvarintField(uint64(0), buf)
	return buf.Bytes()
}

type PartitionRecordWrite struct {
	FrameVersion          int8
	Type                  int8
	Version               int8
	PartitionID           int32
	TopicUUID             uuid.UUID
	ReplicaArray          []int32
	InSyncReplicaArray    []int32
	RemovingReplicasArray []int32
	AddingReplicasArray   []int32
	Leader                int32
	LeaderEpoch           int32
	PartitionEpoch        int32
	DirectoriesArray      []uuid.UUID
	TaggedFieldsCount     uint64
}

func (prw *PartitionRecordWrite) RecordType() int8 {
	return int8(3)
}
func (prw *PartitionRecordWrite) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, prw.FrameVersion)
	binary.Write(buf, binary.BigEndian, prw.Type)
	binary.Write(buf, binary.BigEndian, prw.Version)
	binary.Write(buf, binary.BigEndian, prw.PartitionID)

	buf.Write(prw.TopicUUID[:])
	writeUvarintField(uint64(len(prw.ReplicaArray)+1), buf)
	for _, arr := range prw.ReplicaArray {
		binary.Write(buf, binary.BigEndian, arr)
	}
	writeUvarintField(uint64(len(prw.InSyncReplicaArray)+1), buf)
	for _, arr := range prw.InSyncReplicaArray {
		binary.Write(buf, binary.BigEndian, arr)
	}
	writeUvarintField(uint64(len(prw.RemovingReplicasArray)+1), buf)
	for _, arr := range prw.RemovingReplicasArray {
		binary.Write(buf, binary.BigEndian, arr)
	}
	writeUvarintField(uint64(len(prw.AddingReplicasArray)+1), buf)
	for _, arr := range prw.AddingReplicasArray {
		binary.Write(buf, binary.BigEndian, arr)
	}
	binary.Write(buf, binary.BigEndian, prw.Leader)
	binary.Write(buf, binary.BigEndian, prw.LeaderEpoch)
	binary.Write(buf, binary.BigEndian, prw.PartitionEpoch)
	writeUvarintField(uint64(len(prw.DirectoriesArray)+1), buf)
	for _, arr := range prw.DirectoriesArray {
		buf.Write(arr[:])
	}
	writeUvarintField(uint64(0), buf)
	return buf.Bytes()
}

type FeatureLevelRecordWrite struct {
	FrameVersion      int8
	Type              int8
	Version           int8
	Name              string
	FeatureLevel      int16
	TaggedFieldsCount uint64
}

func (flrw *FeatureLevelRecordWrite) RecordType() int8 {
	return int8(12)
}
func (flrw *FeatureLevelRecordWrite) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, flrw.FrameVersion)
	binary.Write(buf, binary.BigEndian, flrw.Type)
	binary.Write(buf, binary.BigEndian, flrw.Version)
	writeUvarintField(uint64(len(flrw.Name)+1), buf)
	buf.Write([]byte(flrw.Name))
	binary.Write(buf, binary.BigEndian, flrw.FeatureLevel)
	writeUvarintField(uint64(0), buf)
	return buf.Bytes()
}
func createFeatureLevelBatch() *RecordBatch {
	flrw := &FeatureLevelRecordWrite{
		FrameVersion:      int8(1),
		Type:              int8(12),
		Version:           int8(0),
		Name:              "metadata.version",
		FeatureLevel:      int16(20),
		TaggedFieldsCount: uint64(0),
	}
	record := &Record{
		Attributes:        int8(0),
		TimestampDelta:    int64(0),
		OffsetDelta:       int64(0),
		KeyLength:         int64(-1),
		Key:               "",
		Value:             flrw,
		HeadersArrayCount: uint64(0),
	}
	timestamp := time.Now().UnixMilli()
	recordBatch := &RecordBatch{
		PartitionLeaderEpoch: int32(1),
		MagicByte:            int8(2),
		Attributes:           int16(0),
		LastOffsetDelta:      int32(0),
		BaseTimestamp:        timestamp,
		MaxTimestamp:         timestamp,
		ProducerId:           int64(-1),
		ProducerEpoch:        int16(-1),
		BaseSequence:         int32(-1),
		RecordsLength:        int32(1),
		Records:              make([]*Record, 0),
	}
	recordBatch.Records = append(recordBatch.Records, record)
	return recordBatch
}
func createTopicRecordBatch(topicNum int) (*RecordBatch, uuid.UUID) {
	trw := &TopicRecordWrite{
		FrameVersion:      int8(1),
		Type:              int8(2),
		Version:           int8(0),
		TopicName:         fmt.Sprintf("topic-%d", topicNum),
		TopicUUID:         uuid.New(),
		TaggedFieldsCount: uint64(0),
	}
	record := &Record{
		Attributes:        int8(0),
		TimestampDelta:    int64(0),
		OffsetDelta:       int64(0),
		KeyLength:         int64(-1),
		Key:               "",
		Value:             trw,
		HeadersArrayCount: uint64(0),
	}
	timestamp := time.Now().UnixMilli()
	recordBatch := &RecordBatch{
		PartitionLeaderEpoch: int32(1),
		MagicByte:            int8(2),
		Attributes:           int16(0),
		LastOffsetDelta:      int32(0),
		BaseTimestamp:        timestamp,
		MaxTimestamp:         timestamp,
		ProducerId:           int64(-1),
		ProducerEpoch:        int16(-1),
		BaseSequence:         int32(-1),
		RecordsLength:        int32(1),
		Records:              make([]*Record, 0),
	}
	recordBatch.Records = append(recordBatch.Records, record)
	return recordBatch, trw.TopicUUID
}
func createPartitionRecordBatch(topicUUID uuid.UUID, numPartitions int) *RecordBatch {
	allRecords := make([]*Record, 0)
	for i := range numPartitions {
		prw := &PartitionRecordWrite{
			FrameVersion:          int8(1),
			Type:                  int8(3),
			Version:               int8(1),
			PartitionID:           int32(i),
			TopicUUID:             topicUUID,
			ReplicaArray:          []int32{int32(1), int32(2), int32(3)},
			InSyncReplicaArray:    []int32{int32(1), int32(2), int32(3)},
			RemovingReplicasArray: []int32{},
			AddingReplicasArray:   []int32{},
			Leader:                int32(i + 1),
			LeaderEpoch:           int32(0),
			PartitionEpoch:        int32(0),
			DirectoriesArray:      []uuid.UUID{},
			TaggedFieldsCount:     uint64(0),
		}
		record := &Record{
			Attributes:        int8(0),
			TimestampDelta:    int64(0),
			OffsetDelta:       int64(i),
			KeyLength:         int64(-1),
			Key:               "",
			Value:             prw,
			HeadersArrayCount: uint64(0),
		}
		allRecords = append(allRecords, record)
	}
	timestamp := time.Now().UnixMilli()
	recordBatch := &RecordBatch{
		PartitionLeaderEpoch: int32(1),
		MagicByte:            int8(2),
		Attributes:           int16(0),
		LastOffsetDelta:      int32(len(allRecords) - 1),
		BaseTimestamp:        timestamp,
		MaxTimestamp:         timestamp,
		ProducerId:           int64(-1),
		ProducerEpoch:        int16(-1),
		BaseSequence:         int32(-1),
		RecordsLength:        int32(3),
		Records:              allRecords,
	}
	return recordBatch
}

func GenerateClusterMetadata(clusterMetadataDir string, numTopics, brokerId int) error {
	if brokerId != 0 {
		log.Printf("BrokerId %d not controller broker - skipping writing cluster metadata\n", brokerId)
		return nil
	}
	log.Printf("BrokerId %d is the controller broker - proceeding with writing cluster metadata log file\n", brokerId)

	metadataFile, err := os.OpenFile(clusterMetadataDir+"00000000000000000000.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0777)
	if err != nil {
		log.Printf("Error creating / opening cluster metadata file %v\n", err)
		return err
	}
	defer metadataFile.Close()
	recordBatches := make([]*RecordBatch, 0)
	featureLevel := createFeatureLevelBatch()
	recordBatches = append(recordBatches, featureLevel)

	for i := range numTopics {
		topicBatch, topicUUID := createTopicRecordBatch(i)
		partitionBatch := createPartitionRecordBatch(topicUUID, 3)
		recordBatches = append(recordBatches, topicBatch)
		recordBatches = append(recordBatches, partitionBatch)
	}
	for i, ref := range recordBatches {
		batchBytes := ref.Encode(int64(i))
		metadataFile.Write(batchBytes)
	}
	return nil
}
