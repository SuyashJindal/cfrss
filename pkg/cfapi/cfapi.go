// Package cfapi contains the pipe that connects this application to Codeforces.
package cfapi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/variety-jones/cfrss/pkg/models"
)

const (
	baseUrl               = "https://codeforces.com/api"
	recentActionsEndpoint = "/recentActions"

	kStatusOK = "OK"
)

// CodeforcesInterface contains all the methods of the Codeforces API.
type CodeforcesInterface interface {
	RecentActions(maxCount int) ([]models.RecentAction, error)
}

// CodeforcesClient implements the Codeforces interface.
type codeforcesClient struct {
	client http.Client
}

// RecentActions fetches a list of recent blogs/comments from Codeforces.
func (cf *codeforcesClient) RecentActions(maxCount int) (
	[]models.RecentAction, error) {
	zap.S().Info("Executing RecentActions API...")

	// Create the HTTP request and add query parameters.
	url := baseUrl + recentActionsEndpoint
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		zap.S().Debugf("URL: %s", url)
		return nil, fmt.Errorf("could not create request for /recentActions api "+
			"with error [%w]", err)
	}
	query := req.URL.Query()
	query.Add("maxCount", fmt.Sprint(maxCount))
	req.URL.RawQuery = query.Encode()

	// Make the HTTP call.
	resp, err := cf.client.Do(req)
	if err != nil {
		zap.S().Debugf("request: %+v", req)
		return nil, fmt.Errorf("http call to /recentActions failed "+
			"with error [%w]", err)
	}
	defer resp.Body.Close()

	// Read the response body.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		zap.S().Debugf("response: %+v", resp)
		return nil, fmt.Errorf("could not read response of /recentActions "+
			"with error [%w]", err)
	}

	// Unmarshal the response.
	wrapper := struct {
		Status  string
		Comment string
		Result  []models.RecentAction
	}{}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		zap.S().Debugf("body: %s", string(body))
		return nil, fmt.Errorf("could not unmarshal /recentActions response "+
			"with error [%w]", err)
	}

	// Check for internal server errors from Codeforces.
	if wrapper.Status != kStatusOK {
		zap.S().Debugf("response body: %s", string(body))
		return nil, fmt.Errorf("codeforces returned an internal error "+
			"with comment [%s]", wrapper.Comment)
	}
	return wrapper.Result, nil
}

// NewCodeforcesClient returns a concrete implementation of the
// CodeforcesInterface
func NewCodeforcesClient(timeOut time.Duration) CodeforcesInterface {
	cf := new(codeforcesClient)
	cf.client = http.Client{
		Timeout: timeOut,
	}

	return cf
}
