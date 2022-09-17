package web

import (
	"net/http"
	"strconv"

	"go.uber.org/zap"

	"github.com/labstack/echo/v4"

	"github.com/variety-jones/cfrss/pkg/models"
	"github.com/variety-jones/cfrss/pkg/utils"
)

const (
	defaultPageSize = 100
)

func (srv *Server) HomeHandler(c echo.Context) error {
	return c.JSON(http.StatusOK, "OK")
}

func (srv *Server) ListenAndServe(addr string) error {
	zap.S().Infof("Starting the web server at %s", addr)
	return srv.ec.Start(addr)
}

func (srv *Server) UserSignup(c echo.Context) error {
	zap.S().Info("Executing UserSignup handler...")

	username := c.FormValue("username")
	password := c.FormValue("password")

	user := &models.User{
		Uuid:           utils.GetNewUUID(),
		Username:       username,
		HashedPassword: password,
	}

	if err := srv.cfStore.AddUser(user); err != nil {
		zap.S().Errorf("Could not register user %s with error [%+v]",
			username, err)
		return c.JSON(http.StatusBadRequest,
			http.StatusText(http.StatusBadRequest))
	}

	return c.JSON(http.StatusOK, user)
}

func (srv *Server) SubscribeToBlogs(c echo.Context) error {
	zap.S().Info("Executing SubscribeToBlogs handler...")

	uuid := c.FormValue("uuid")

	// TODO: Switch to array based methods.
	blogsIDs, err := strconv.Atoi(c.FormValue("blogIDs"))
	if err != nil {
		zap.S().Errorf("Could not extract blog IDs from [%v] with error [%+v]",
			blogsIDs, err)
		return c.JSON(http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError))
	}

	if err := srv.cfStore.SubscribeToBlogs(uuid, blogsIDs); err != nil {
		zap.S().Errorf("User %s could not subscribe to blogs %v "+
			"with error [%+v]", uuid, blogsIDs, err)
		return c.JSON(http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError))
	}

	return c.JSON(http.StatusOK, http.StatusText(http.StatusOK))
}

func (srv *Server) UnsubscribeFromBlogs(c echo.Context) error {
	zap.S().Info("Executing UnsubscribeFromBlogs handler...")

	uuid := c.FormValue("uuid")

	// TODO: Switch to array based methods.
	blogsIDs, err := strconv.Atoi(c.FormValue("blogIDs"))
	if err != nil {
		zap.S().Errorf("Could not extract blog IDs from [%+v] with error [%v]",
			blogsIDs, err)
		return c.JSON(http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError))
	}

	if err := srv.cfStore.UnsubscribeFromBlogs(uuid, blogsIDs); err != nil {
		zap.S().Infof("User %s could not unsubscribe from blogs %v "+
			"with error [%+v]", uuid, blogsIDs, err)
		return c.JSON(http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError))
	}

	return c.JSON(http.StatusOK, http.StatusText(http.StatusOK))
}

func (srv *Server) QueryRecentActions(c echo.Context) error {
	zap.S().Info("Executing QueryRecentActions handler...")

	startTimestamp, err := strconv.ParseInt(c.FormValue("startTimestamp"),
		10, 64)
	if err != nil {
		zap.S().Errorf("Could not parse startTimestamp with error [%+v]", err)
		return c.JSON(http.StatusBadRequest,
			http.StatusText(http.StatusBadRequest))
	}

	actions, err := srv.cfStore.QueryRecentActions(startTimestamp, defaultPageSize)
	if err != nil {
		zap.S().Errorf("Querying of recent actions failed with error [%+v]", err)
		return c.JSON(http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError))
	}

	return c.JSON(http.StatusOK, actions)
}

func (srv *Server) QueryCommentsFromBlog(c echo.Context) error {
	zap.S().Info("Executing QueryCommentsFromBlog handler...")

	startTimestamp, err := strconv.ParseInt(c.FormValue("startTimestamp"),
		10, 64)
	if err != nil {
		zap.S().Errorf("Could not parse startTimestamp with error [%+v]", err)
		return c.JSON(http.StatusBadRequest,
			http.StatusText(http.StatusBadRequest))
	}

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		zap.S().Errorf("Could not parse id from parameters with error [%+v]",
			err)
		return c.JSON(http.StatusBadRequest,
			http.StatusText(http.StatusBadRequest))
	}

	comments, err := srv.cfStore.QueryCommentsFromBlog(id, startTimestamp, defaultPageSize)
	if err != nil {
		zap.S().Errorf("Querying of comments failed with error [%+v]", err)
		return c.JSON(http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError))
	}

	return c.JSON(http.StatusOK, comments)
}

func (srv *Server) QueryRecentActionsForUser(c echo.Context) error {
	zap.S().Info("Executing QueryRecentActionsFromUser handler...")

	uuid := c.FormValue("uuid")
	startTimestamp, err := strconv.ParseInt(c.FormValue("startTimestamp"),
		10, 64)
	if err != nil {
		zap.S().Errorf("Could not parse startTimestamp with error [%+v]", err)
		return c.JSON(http.StatusBadRequest,
			http.StatusText(http.StatusBadRequest))
	}

	actions, err := srv.cfStore.QueryRecentActionsForUser(uuid, startTimestamp,
		defaultPageSize)
	if err != nil {
		zap.S().Errorf("Querying of recent actions for user %s failed "+
			"with error [%+v]", uuid, err)
		return c.JSON(http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError))
	}

	return c.JSON(http.StatusOK, actions)
}
