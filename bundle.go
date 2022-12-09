package astra

import (
	"archive/zip"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"strings"
)

const astraURLSuffix = "apps.astra.datastax.com:443"

type bundle struct {
	tlsConfig *tls.Config
	host      string
	port      int
}

func loadBundleZip(reader *zip.Reader) (*bundle, error) {
	contents, err := extract(reader)
	if err != nil {
		return nil, err
	}

	config := struct {
		Host             string `json:"host"`
		Port             int    `json:"port"`
		KeyStorePassword string `json:"keyStorePassword"`
	}{}
	err = json.Unmarshal(contents["config.json"], &config)
	if err != nil {
		return nil, err
	}

	// rootCAs := x509.NewCertPool()
	rootCAs, err := createCertPool()
	if err != nil {
		return nil, err
	}

	ok := rootCAs.AppendCertsFromPEM(contents["ca.crt"])
	if !ok {
		return nil, fmt.Errorf("the provided CA cert could not be added to the root CA pool")
	}

	cert, err := tls.X509KeyPair(contents["cert"], contents["key"])
	if err != nil {
		return nil, err
	}

	var astraURI string
	if strs := strings.Split(config.Host, "."); len(strs) > 1 {
		astraURI = fmt.Sprintf("%s.%s", strs[0], astraURLSuffix)
	} else {
		return nil, fmt.Errorf("invalid host name: %s", config.Host)
	}

	return &bundle{
		tlsConfig: &tls.Config{
			RootCAs:      rootCAs,
			Certificates: []tls.Certificate{cert},
			ServerName:   strings.Split(astraURI, ":")[0],
		},
		host: astraURI,
	}, nil
}

func loadBundleZipFromPath(path string) (*bundle, error) {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}

	defer func(reader *zip.ReadCloser) {
		_ = reader.Close()
	}(reader)

	return loadBundleZip(&reader.Reader)
}

func extract(reader *zip.Reader) (map[string][]byte, error) {
	contents := make(map[string][]byte)

	for _, file := range reader.File {
		switch file.Name {
		case "config.json", "cert", "key", "ca.crt":
			bytes, err := loadBytes(file)
			if err != nil {
				return nil, err
			}
			contents[file.Name] = bytes
		}
	}

	for _, file := range []string{"config.json", "cert", "key", "ca.crt"} {
		if _, ok := contents[file]; !ok {
			return nil, fmt.Errorf("bundle missing '%s' file", file)
		}
	}

	return contents, nil
}

func loadBytes(file *zip.File) ([]byte, error) {
	r, err := file.Open()
	if err != nil {
		return nil, err
	}
	res, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	return res, nil
}

func createCertPool() (*x509.CertPool, error) {
	ca, err := x509.SystemCertPool()
	if err != nil && runtime.GOOS == "windows" {
		return x509.NewCertPool(), nil
	}
	return ca, err
}
