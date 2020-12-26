package esclient

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

// ESClient is used for all data handling with the elasticsearch database
type ESClient struct {
	Host       string
	User       string
	Password   string
	HTTPClient http.Client
}

// IndexExists is used to check weather an index in the database exists or not
func (e *ESClient) IndexExists(index string) (bool, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", e.Host, index), nil)
	if err != nil {
		log.Print(err)
		return false, err
	}

	req.SetBasicAuth(e.User, e.Password)

	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		log.Print(err)
		return false, err
	}

	log.Print(resp.StatusCode)

	if resp.StatusCode != 200 {
		return false, nil
	}
	return true, nil
}

// GetDocument is the raw functions used to get a document from the ES database
func (e *ESClient) GetDocument(index string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", e.Host, index), nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(e.User, e.Password)

	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Could not get document: %b", resp.StatusCode)
	}

	return resp.Body, err
}

// CreateIndex is used to create an index in the database
// mappings can be nil, in that case no mappings are made, and ES decides it themselves
// An error is returned if it already exists
func (e *ESClient) CreateIndex(index string, mappings string) error {
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/%s", e.Host, index), strings.NewReader(mappings))

	if err != nil {
		return err
	}

	req.SetBasicAuth(e.User, e.Password)

	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("Could not create index StatusCode: %b", resp.StatusCode)
	}

	return nil
}

// CreateDocument is used to post a document to the services
func (e *ESClient) CreateDocument(index string, document string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s/_doc", e.Host, index), strings.NewReader(document))
	if err != nil {
		return err
	}

	req.SetBasicAuth(e.User, e.Password)
	req.Header.Add("Content-Type", "application/json")

	log.Print("Calling create document API")

	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return err
	}

	log.Print(resp.StatusCode)

	if resp.StatusCode != 201 {
		return fmt.Errorf("Could not create document StatusCode: %b", resp.StatusCode)
	}

	return nil
}
