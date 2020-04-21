package main

import (
	"errors"
	"strings"
)


func listPackageReply(key []string, etag []string) string {
	var buf strings.Builder
	for i := range key {
		buf.WriteString(etag[i])
		buf.WriteString("|")
		buf.WriteString(key[i])
		buf.WriteString("\n")
	}
	return buf.String()
}

func list(args []string) (reply string, err error) {
	if len(args) != 2 {
		err = errors.New("LIST requires param (bucket, prefix)")
		return
	}

	bucket, prefix := args[0], args[1]
	store := getStore(bucket)
	if key, etag, ok := store.retrieve(prefix); ok {
		reply = listPackageReply(key, etag)
		return
	}

	key := make([]string, 0, 20)
	etag := make([]string, 0, 20)
	err = s3ListObjects(bucket, prefix, func(k, t string) {
		if k[len(k)-1] == '/' {
			// skip DIR
			return
		}
		key = append(key, k)
		etag = append(etag, t)
	})

	if err != nil {
		return
	}

	store.insert(prefix, key, etag)
	reply = listPackageReply(key, etag)
	return
}



func invalidate(args []string) (reply string, err error) {
	if len(args) != 1 {
		err = errors.New("INVALIDATE requires param (bucket)")
		return
	}

	bucket := args[0]
	storeLock.Lock()
	delete(storeList, bucket)
	storeLock.Unlock()
	reply = ""
	return
}


func setETag(args []string) (reply string, err error) {
	if len(args) != 3 {
		err = errors.New("SETETAG requires param (bucket, key, etag)")
	}

	bucket, key, etag := args[0], args[1], args[2]
	store := getStore(bucket)
	store.setETag(key, etag)
	reply = ""
	return
}


func getETag(args []string) (reply string, err error) {
	if len(args) != 2 {
		err = errors.New("GETETAG requires param (bucket, key)")
	}

	bucket, key := args[0], args[1]
	store := getStore(bucket)
	reply = store.getETag(key)
	return
}


func deleteKey(args []string) (reply string, err error) {
	if len(args) != 2 {
		err = errors.New("DELETE requires param (bucket, key)")
	}

	bucket, key := args[0], args[1]
	store := getStore(bucket)
	store.setETag(key, "")
	reply = ""
	return
}
