package mapreduce

import (
	"log"
	"encoding/json"
	"sort"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// 1) Each reduceTask will eat nMask files and produce 1 output file***
	// 2) Create the new file and append to the file

	log.Println("[doReduce] parameters", jobName, reduceTask, outFile, nMap)

	m := make(map[string][]string)

	// 1) read the file
	for mapTask := 0; mapTask < nMap; mapTask++ {
		// read file
		inFileName := reduceName(jobName, mapTask, reduceTask)
		inFile, err := os.Open(inFileName)
		if err != nil {
			log.Fatal(err)
		}

		// decoder
		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
		inFile.Close()
	}

	// 2) sort keys
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 3) new & append
	f, err := os.Create(outFile)
	f.Close()
	f, err = os.OpenFile(outFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(f)

	// 4)  run reduceF()
	for _, k := range keys {
		enc.Encode(KeyValue{k, reduceF(k, m[k])})
	}
	f.Close()

}
