package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

type InMemoryResource struct {
	id   int         `json:"id"`
	data interface{} `json:"data"`
}

func (imr *InMemoryResource) ID() int {
	return imr.id
}

type InMemoryResourceManager struct {
	data map[int]*InMemoryResource
}

func NewInMemoryResourceManager() *InMemoryResourceManager {
	return &InMemoryResourceManager{
		data: make(map[int]*InMemoryResource),
	}
}

func (mrm *InMemoryResourceManager) NewResource() Resource {
	return new(InMemoryResource)
}

func (mrm *InMemoryResourceManager) Insert(data ...Resource) error {
	// loop over one or more provided resources
	for i := range data {
		// get pointer to each one at a time
		resource := data[i]
		// check to see if it already exists
		if _, ok := mrm.data[resource.ID()]; !ok {
			// insert int into the table if it does not exist
			mrm.data[resource.ID()] = resource.(*InMemoryResource)
		}
	}
	return nil
}

func (mrm *InMemoryResourceManager) Update(data ...Resource) error {
	// loop over one or more provided resources
	for i := range data {
		// get pointer to each one at a time
		resource := data[i]
		// just write the resource to the table, overwriting any old ones
		mrm.data[resource.ID()] = resource.(*InMemoryResource)
	}
	return nil
}

func (mrm *InMemoryResourceManager) Delete(ids ...int) error {
	// check nil list
	if ids == nil {
		// remove all
		for id, _ := range mrm.data {
			delete(mrm.data, id)
		}
		return nil
	}
	// loop over one or more provided resources
	for _, id := range ids {
		// check to see if it exists
		if _, ok := mrm.data[id]; ok {
			// if it does, delete that data resource entry
			delete(mrm.data, id)
		}
	}
	return nil
}

func (mrm *InMemoryResourceManager) Search(ids ...int) ([]Resource, error) {
	// make resource list to return
	var resources []Resource
	// check nil list
	if ids == nil {
		// add all to list
		for _, resource := range mrm.data {
			resources = append(resources, resource)
		}
		return resources, nil
	}
	// loop over one or more provided resources
	for _, id := range ids {
		// check to see if it exists
		if resource, ok := mrm.data[id]; ok {
			// if it does, add it to the list
			resources = append(resources, resource)
		}
	}
	return resources, nil
}

type Resource interface {
	ID() int
	// Name() string
	//MarshalBinary() (data []byte, err error)
	//UnmarshalBinary(data []byte) error
}

// ResourceManager is an interface to provide a generic
// manager of data resources. It is meant to be implemented
// by the user using this package.
type ResourceManager interface {

	// NewResource method should return a new empty data resource.
	// It should not add or modify the underlying storage engine
	// in any way.
	NewResource() Resource

	// Insert method should take the data provided, add it to
	// the storage engine and return a nil error on success.
	Insert(data ...Resource) error

	// Update method should take the data provided, find the
	// matching data resource entries, and update them in the
	// underlying storage engine. It is expected to return a
	// nil error on success.
	Update(data ...Resource) error

	// Delete method should remove one or all of the data resource
	// entries from the underlying storage engine and return a nil
	// error on success.
	Delete(ids ...int) error

	// Search method should return one or all of the data resource
	// entries from the underlying storage engine. It is also expected
	// to return a nil error on success.
	Search(ids ...int) ([]Resource, error)
}

const byIdRegx = `\/*([A-z\-\_]*)\/*%s\/*[0-9]+`
const byReRegx = `^\/*([A-z\-\_]*)$`

type API struct {
	resourceName string
	manager      ResourceManager
	byID         *regexp.Regexp
	byRe         *regexp.Regexp
}

func NewAPI(resourceName string, manager ResourceManager) *API {
	return &API{
		resourceName: resourceName,
		manager:      manager,
		byID:         regexp.MustCompile(fmt.Sprintf(byIdRegx, resourceName)),
		byRe:         regexp.MustCompile(byReRegx),
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if api.byID.MatchString(r.URL.Path) {
			api.returnByID(w, r)
		}
		if api.byRe.MatchString(r.URL.Path) {
			api.returnAll(w, r)
		}
		http.NotFoundHandler()
	case http.MethodPost:
		api.insert(w, r)
	case http.MethodPut:
		api.updateByID(w, r)
	case http.MethodDelete:
		if api.byID.MatchString(r.URL.Path) {
			api.deleteByID(w, r)
		}
		if api.byRe.MatchString(r.URL.Path) {
			api.deleteAll(w, r)
		}
	case http.MethodOptions:
		api.info(w, r)
	default:
		api.notFound(w, r)
	}
}

func (api *API) info(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%s", api.manager)
	return
}

func (api *API) notFound(w http.ResponseWriter, r *http.Request) {
	Response(w, http.StatusNotFound)
	return
}

// insert example: POST -> example.com/{resource}
func (api *API) insert(w http.ResponseWriter, r *http.Request) {
	// read the data that has been posted to this endpoint
	body, err := io.ReadAll(r.Body)
	if err != nil {
		Response(w, http.StatusBadRequest)
		return
	}
	// create a new data resource instance
	res := api.manager.NewResource()
	// fill the empty data resource out
	err = json.Unmarshal(body, res)
	if err != nil {
		Response(w, http.StatusBadRequest)
		return
	}
	// add the newly filled out resource to the storage engine
	err = api.manager.Insert(res)
	if err != nil {
		Response(w, http.StatusBadRequest)
		return
	}
	// otherwise, we are good!
	Response(w, http.StatusOK)
	return
}

func getIDFromPath(path string) int {
	ss := strings.Split(path, "/")
	for _, s := range ss {
		id, err := strconv.Atoi(s)
		if err == nil {
			return id
		}
	}
	return -1
}

// returnByID example: GET -> example.com/{resource}/{id}
func (api *API) returnByID(w http.ResponseWriter, r *http.Request) {
	// get id from the url
	id := getIDFromPath(r.URL.Path)
	if id < 0 {
		Response(w, http.StatusBadRequest)
		return
	}
	// use the manager to find the data resource by id
	res, err := api.manager.Search(id)
	if err != nil || len(res) != 1 {
		Response(w, http.StatusNotFound)
		return
	}
	// respond with the data
	ResponseWithData(w, http.StatusOK, res)
	return
}

// returnAll example: GET -> example.com/{resource}
func (api *API) returnAll(w http.ResponseWriter, r *http.Request) {
	// use the manager to find and return all the data resources
	res, err := api.manager.Search(nil...)
	if err != nil {
		Response(w, http.StatusNotFound)
		return
	}
	// respond with data
	ResponseWithData(w, http.StatusOK, res)
	return
}

// deleteOne example: DELETE -> example.com/{resource}/{id}
func (api *API) deleteByID(w http.ResponseWriter, r *http.Request) {
	// get id from the url
	id := getIDFromPath(r.URL.Path)
	if id < 0 {
		Response(w, http.StatusBadRequest)
		return
	}
	// use the manager to delete the specified data resouce
	err := api.manager.Delete(id)
	if err != nil {
		Response(w, http.StatusNotFound)
		return
	}
	// respond with data
	Response(w, http.StatusOK)
	return
}

// deleteAll example: DELETE -> example.com/{resource}
func (api *API) deleteAll(w http.ResponseWriter, r *http.Request) {
	// attempt to delete all the data resources
	err := api.manager.Delete(nil...)
	if err != nil {
		Response(w, http.StatusInternalServerError)
		return
	}
	// otherwise, we are good!
	Response(w, http.StatusOK)
}

// updateOne example: PUT -> example.com/{resource}/{id}
func (api *API) updateByID(w http.ResponseWriter, r *http.Request) {
	// get id from the url
	id := getIDFromPath(r.URL.Path)
	if id < 0 {
		Response(w, http.StatusBadRequest)
		return
	}
	// read the data that has been posted to this endpoint
	body, err := io.ReadAll(r.Body)
	if err != nil {
		Response(w, http.StatusBadRequest)
		return
	}
	// create a new data resource instance
	res := api.manager.NewResource()
	// fill the empty data resource out
	err = json.Unmarshal(body, res)
	if err != nil {
		Response(w, http.StatusBadRequest)
		return
	}
	// update the specified resource
	err = api.manager.Update(res)
	if err != nil {
		Response(w, http.StatusInternalServerError)
		return
	}
	// otherwise, we are good
	ResponseWithData(w, http.StatusOK, res)
	return
}

func Response(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	data, err := json.Marshal(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{
		Code:    code,
		Message: http.StatusText(code),
	})
	if err != nil {
		code := http.StatusInternalServerError
		http.Error(w, http.StatusText(code), code)
		return
	}
	fmt.Fprintln(w, data)
}

func ResponseWithData(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	data, err := json.Marshal(struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data"`
	}{
		Code:    code,
		Message: http.StatusText(code),
		Data:    data,
	})
	if err != nil {
		code := http.StatusInternalServerError
		http.Error(w, http.StatusText(code), code)
		return
	}
	fmt.Fprintln(w, data)
}
