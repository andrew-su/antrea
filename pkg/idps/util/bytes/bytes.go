// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package bytes

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"golang.org/x/crypto/openpgp" // nolint
)

func Sha256Checksum(in []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(in))
}

func DecryptGPGBytes(in []byte, key []byte) ([]byte, error) {
	keyRing, _ := openpgp.ReadKeyRing(bytes.NewReader(key))
	md, err := openpgp.ReadMessage(bytes.NewReader(in), keyRing, nil, nil)
	if err != nil {
		return nil, err
	}
	out, err := ioutil.ReadAll(md.UnverifiedBody)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ExactTargetFileFromTarGzBytes(in []byte, filename string) ([]byte, *tar.Header, error) {
	gzipReader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, nil, err
	}
	tarReader := tar.NewReader(gzipReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if header.Typeflag == tar.TypeReg && header.Name == filename {
			out, err := ioutil.ReadAll(tarReader)
			if err != nil {
				return nil, nil, err
			}
			return out, header, err
		}
	}
	return nil, nil, fmt.Errorf("target filenamne %s was not found", filename)
}

func ExactTargetDirFromTarGzBytes(in []byte, dirname string) (map[*tar.Header][]byte, error) {
	outs := make(map[*tar.Header][]byte)
	gzipReader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	tarReader := tar.NewReader(gzipReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if header.Typeflag == tar.TypeReg && strings.HasPrefix(header.Name, dirname) {
			out, err := ioutil.ReadAll(tarReader)
			if err != nil {
				return nil, err
			}
			outs[header] = out
		}
	}
	return outs, nil
}

func ExactTargetFileFromZipBytes(in []byte, filename string) ([]byte, error) {
	zipReader, err := zip.NewReader(bytes.NewReader(in), int64(len(in)))
	if err != nil {
		return nil, err
	}
	var zipFile *zip.File
	for _, zf := range zipReader.File {
		if zf.Name == filename {
			zipFile = zf
			break
		}
	}

	if zipFile != nil {
		f, err := zipFile.Open()
		if err != nil {
			return nil, err
		}
		defer f.Close() // nolint
		return ioutil.ReadAll(f)
	}

	return nil, fmt.Errorf("target filenamne %s was not found", filename)
}
