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
	"sync"
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

type chunk struct {
	cacheFile      string
	finalFile      string
	url            string
	expectedDigest string
}

func (d *downloader) readChunk(ch chan chunk) {
	defer d.wg.Done()
	for {
		data, ok := <-ch
		if !ok {
			break
		}

		fmt.Printf("Downloading %s to %s\n", data.url, data.cacheFile)

		// TODO: parallelise big files to chunks?
		partFile := fmt.Sprintf("%s.%d", data.cacheFile, rand.Int())
		digest, err := downloadFile(partFile, data.url)
		if err != nil {
			panic(err)
		}
		if digest != data.expectedDigest {
			panic(fmt.Errorf("Mismatching digest: Downloaded %s, sha256 was %s", data.url, digest))
		}
		err = os.Rename(partFile, data.cacheFile)
		if err != nil {
			panic(err)
		}
		err = makeHardlink(data.cacheFile, data.finalFile)
		if err != nil {
			panic(err)
		}
	}
}

type downloader struct {
	wg          sync.WaitGroup
	cachePath   string
	concurrency int
}

func makeHardlink(cacheFile, finalFile string) error {
	err := os.MkdirAll(path.Dir(finalFile), 0755)
	if err != nil {
		return err
	}
	err = os.Link(cacheFile, finalFile)
	if err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

type Downloader interface {
	Download(downloadWorkload, server, outputDirectory string) error
}

func NewDownloader(cachePath string, concurrency int) Downloader {
	return &downloader{
		cachePath:   cachePath,
		concurrency: concurrency,
	}
}

// Download downloads files from server and writes to outputDirectory, looking
// up and adding to cachePath as needed.
func (d *downloader) Download(downloadWorkload, server, outputDirectory string) error {
	os.MkdirAll(d.cachePath, 0755)
	file, err := os.Open(downloadWorkload)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	ch := make(chan chunk, d.concurrency)
	for i := 0; i < d.concurrency; i++ {
		d.wg.Add(1)
		go d.readChunk(ch)
	}

	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		cacheFile := path.Join(d.cachePath, words[0])
		finalFile := path.Join(outputDirectory, words[1])
		if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
			url := server + words[0]
			ch <- chunk{cacheFile: cacheFile, url: url, expectedDigest: words[0], finalFile: finalFile}
		} else if err != nil {
			return err
		} else {
			err = makeHardlink(cacheFile, finalFile)
			if err != nil {
				return err
			}
		}
	}
	close(ch)
	d.wg.Wait()

	return scanner.Err()
}
