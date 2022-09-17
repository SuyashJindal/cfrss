package web_test

import (
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/labstack/echo/v4"

	"github.com/variety-jones/cfrss/pkg/cfapi"
	"github.com/variety-jones/cfrss/pkg/scheduler"
	"github.com/variety-jones/cfrss/pkg/store"
	"github.com/variety-jones/cfrss/pkg/web"
)

var _ = Describe("WebServer", func() {
	inMemoryStore := store.NewInMemoryCodeforcesStore()
	dummyCfClient := cfapi.NewDummyCodeforcesClient()
	dummyScheduler := scheduler.NewScheduler(dummyCfClient, inMemoryStore,
		100, 1*time.Second)

	for cnt := 0; cnt <= 100; cnt++ {
		dummyScheduler.Sync()
	}

	e := echo.New()
	rec := httptest.NewRecorder()

	webServer := web.CreateWebServer(inMemoryStore)

	It("should successfully register a new user", func() {
		httpReq, _ := http.NewRequest(http.MethodPost,
			"/user/signup", nil)

		validQ := httpReq.URL.Query()
		validQ.Add("username", "fake-user")
		validQ.Add("password", "fake-password")

		httpReq.URL.RawQuery = validQ.Encode()
		c := e.NewContext(httpReq, rec)
		Expect(webServer.UserSignup(c)).Should(BeNil())
		Expect(rec.Code).Should(Equal(http.StatusOK))
	})

	It("should return ")

})
