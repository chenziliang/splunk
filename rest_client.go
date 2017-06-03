package splunk

import (
	"bytes"
	"crypto/tls"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
)

type RestClient struct {
	serverURI  string
	sessionKey string
	client     *http.Client
}

func NewRestClient(serverURI, sessionKey string, maxKeepAliveConn, timeout int) (*RestClient, error) {
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConnsPerHost: maxKeepAliveConn,
	}
	client := &http.Client{Transport: tr, Timeout: time.Duration(timeout) * time.Second}

	return &RestClient{
		serverURI:  serverURI,
		sessionKey: sessionKey,
		client:     client,
	}, nil
}

func (rest *RestClient) Request(
	path string, method string, headers map[string]string, data []byte,
	retries int) ([]byte, error) {
	var reader io.Reader
	if data != nil {
		reader = bytes.NewBuffer(data)
	}

	var res []byte
	var err error
	serverURI := rest.serverURI + path
	for i := 0; i < retries; i++ {
		res, err = rest.doRequest(serverURI, method, headers, reader)
		if err == nil {
			return res, err
		} else {
			time.Sleep(time.Second)
		}
	}
	return res, err
}

func (rest *RestClient) doRequest(
	serverURI, method string, headers map[string]string, reader io.Reader) ([]byte, error) {
	req, err := http.NewRequest(method, serverURI, reader)
	if err != nil {
		return nil, err
	}

	rest.addHeaders(req, headers)
	resp, err := rest.client.Do(req)
	if err != nil {
		return nil, err
	}

	res, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return nil, errors.New(string(res))
	}
	return res, nil
}

func (rest *RestClient) Login(username, password string) (string, error) {
	data := url.Values{}
	data.Add("username", username)
	data.Add("password", password)
	cred := []byte(data.Encode())
	res, err := rest.Request("/services/auth/login", "POST", nil, cred, 3)
	if err != nil {
		return "", err
	}

	type sessionKey struct {
		SessionKey string `xml:"sessionKey"`
	}
	var key sessionKey
	err = xml.Unmarshal(res, &key)
	if err != nil {
		return "", err
	}

	return key.SessionKey, nil
}

func (rest *RestClient) addHeaders(req *http.Request, headers map[string]string) {
	hasContentType := false
	if headers != nil {
		for k, v := range headers {
			req.Header.Add(k, v)
		}

		if headers["Content-Type"] != "" || headers["content-type"] != "" {
			hasContentType = true
		}
	}

	if !hasContentType {
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	}

	req.Header.Add("Authorization", "Splunk "+rest.sessionKey)
}
