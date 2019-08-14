package pkg

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strings"
)

func downloadFile(filepath, url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	out, err := os.Create(filepath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	checksum := sha256.New()
	body := io.TeeReader(resp.Body, checksum)
	_, err = io.Copy(out, body)
	if err != nil {
		return "", nil
	}
	return hex.EncodeToString(checksum.Sum(nil)), nil
}

// Download downloads files from server and writes to outputDirectory, looking
// up and adding to cachePath as needed.
func Download(downloadWorkload, server, cachePath, outputDirectory string) error {
	os.MkdirAll(cachePath, 0755)
	file, err := os.Open(downloadWorkload)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// TODO: Parallelise over files, parallelise big files to chunks.
	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		cacheFile := path.Join(cachePath, words[0])
		if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
			partFile := fmt.Sprintf("%s.%d", cacheFile, rand.Int())
			url := server + words[0]
			fmt.Printf("Downloading %s to %s\n", url, words[1])
			digest, err := downloadFile(partFile, url)
			if err != nil {
				return err
			}
			if digest != words[0] {
				return fmt.Errorf("Mismatching digest: Downloaded %s, sha256 was %s", url, digest)
			}
			err = os.Rename(partFile, cacheFile)
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		finalFile := path.Join(outputDirectory, words[1])
		err = os.MkdirAll(path.Dir(finalFile), 0755)
		if err != nil {
			return err
		}
		err = os.Link(cacheFile, finalFile)
		if err != nil && !os.IsExist(err) {
			return err
		}
	}

	return scanner.Err()
}
