package user

import (
	"github.com/ivanmalyi/WebService/internal/handlers"
	"github.com/ivanmalyi/WebService/pkg/logging"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

const (
	usersUrl = "/users"
	userUrl  = "/users/:uuid"
)

type handler struct {
	logger logging.Logger
}

func NewHandler(logger logging.Logger) handlers.Handler {
	return &handler{
		logger: logger,
	}
}

func (h *handler) Register(router *httprouter.Router) {
	router.GET(usersUrl, h.GetList)
	router.POST(usersUrl, h.CreateUser)
	router.GET(userUrl, h.GetUserByUIID)
	router.PUT(userUrl, h.UpdateUser)
	router.PATCH(userUrl, h.PartiallyUpdateUser)
	router.DELETE(userUrl, h.DeleteUser)
}

func (h *handler) GetList(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.WriteHeader(200)
	w.Write([]byte("this is list of users"))
}

func (h *handler) CreateUser(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.WriteHeader(201)
	w.Write([]byte("this is CreateUser"))
}

func (h *handler) GetUserByUIID(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.WriteHeader(200)
	w.Write([]byte("this is GetUserByUIID"))
}

func (h *handler) UpdateUser(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.WriteHeader(204)
	w.Write([]byte("this is UpdateUser"))
}

func (h *handler) PartiallyUpdateUser(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.WriteHeader(204)
	w.Write([]byte("this is PartiallyUpdateUser"))
}

func (h *handler) DeleteUser(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.WriteHeader(204)
	w.Write([]byte("this is DeleteUser"))
}
