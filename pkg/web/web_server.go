package web

import (
	"github.com/labstack/echo/v4"

	"github.com/variety-jones/cfrss/pkg/store"
)

type Server struct {
	ec      *echo.Echo
	cfStore store.CodeforcesStore
}

func CreateWebServer(cfStore store.CodeforcesStore) *Server {
	srv := &Server{
		ec:      echo.New(),
		cfStore: cfStore,
	}

	srv.ec.Static("/", "frontend/build")

	v1Public := srv.ec.Group(v1PublicGroup)

	// Public routes.
	v1Public.GET(kHome, srv.HomeHandler)

	v1Public.GET(kRecentActions, srv.QueryRecentActions)
	v1Public.GET(kCommentsFromBlog, srv.QueryCommentsFromBlog)

	v1Public.POST(kUserSignup, srv.UserSignup)

	// Protected routes.

	v1Public.POST(kSubscribeToBlogs, srv.SubscribeToBlogs)
	v1Public.POST(kUnsubscribeFromBlogs, srv.UnsubscribeFromBlogs)

	v1Public.GET(kRecentActionsForUser, srv.QueryRecentActionsForUser)

	return srv
}
