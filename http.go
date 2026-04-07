/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"accedo.io/groupcache/v2/consistenthash"
	pb "accedo.io/groupcache/v2/groupcachepb"
	"google.golang.org/protobuf/proto"
)

const (
	RemoteLoadSourcePeerHeader = "X-Groupcache-Remote-Load-Source-Peer"
	ErrorTypeHeader            = "X-Groupcache-Error-Type"
)

type remoteLoadMetadataContextKey struct{}

type IncomingRemoteLoadMetadata struct {
	IsRemoteLoad bool
	Group        string
	Key          string
	SourcePeer   string

	RecursiveRemoteLoadAllowed bool
}

type BadGroupcacheRequestError struct {
	message string
}

type GroupNotFoundError struct {
	group string
}

type RemoteLoadError struct {
	Group string
	Key   string

	StatusCode int
	Status     string
	Body       []byte

	ErrorType string
	Err       error
}

type RecursiveRemoteLoadForbiddenError struct {
	group string
	key   string

	determinedDestinationPeer string
	sourcePeer                string

	recursiveRemoteLoadAllowed bool
}

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	// opts specifies the options.
	opts HTTPPoolOptions

	mu          sync.Mutex // guards peers and httpGetters
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash

	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func(context.Context) http.RoundTripper

	// Context optionally specifies a context for the server to use when it
	// receives a request.
	// If nil, uses the http.Request.Context()
	Context func(*http.Request) context.Context

	// ServerErrorHandler optionally specifies a function that will serialize the error that occurred during the remote load and forward it to the requesting
	// peer. It may be deserialized on the peer side using a custom PeerErrorHandler if needed.
	ServerErrorHandler func(context.Context, http.ResponseWriter, *http.Request, error)

	// If true, a remote load coming from node A into node B - that node B determines to be part of the key ring of another node (either A, C or any other) -
	// will be forwarded to that other node as a subsequent remote load. This may lead to looping in case A thinks B owns a certain key, while B thinks A owns
	// it.
	AllowRecursiveRemoteLoad bool
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)

	if p.opts.ServerErrorHandler == nil {
		p.opts.ServerErrorHandler = DefaultServerErrorHandler
	}

	RegisterPeerPicker(func() PeerPicker { return p })
	return p
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{
			getTransport: p.opts.Transport,
			selfAddr:     p.self,
			baseURL:      peer + p.opts.BasePath,
		}
	}
}

// GetAll returns all the peers in the pool
func (p *HTTPPool) GetAll() []ProtoGetter {
	p.mu.Lock()
	defer p.mu.Unlock()

	var i int
	res := make([]ProtoGetter, len(p.httpGetters))
	for _, v := range p.httpGetters {
		res[i] = v
		i++
	}
	return res
}

func (p *HTTPPool) KeyOwners() []consistenthash.KeyOwner {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peers.KeyOwners()
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	var ctx context.Context
	if p.opts.Context != nil {
		ctx = p.opts.Context(r)
	} else {
		ctx = r.Context()
	}

	// Parse request.
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		p.opts.ServerErrorHandler(ctx, w, r, BadGroupcacheRequestError{message: "invalid request URL (missing path parts)"})
		return
	}
	groupName := parts[0]
	key := parts[1]

	ctx = p.setIncomingRemoteLoadMetadataOnContext(ctx, r.Header, groupName, key)

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		p.opts.ServerErrorHandler(ctx, w, r, GroupNotFoundError{group: groupName})
		return
	}

	group.Stats.ServerRequests.Add(1)

	// Delete the key and return 200
	if r.Method == http.MethodDelete {
		group.localRemove(key)
		return
	}

	var b []byte

	value := AllocatingByteSliceSink(&b)

	err := group.Get(ctx, key, value)
	if err != nil {
		p.opts.ServerErrorHandler(ctx, w, r, err)
		return
	}

	view, err := value.view()
	if err != nil {
		p.opts.ServerErrorHandler(ctx, w, r, err)
		return
	}
	var expireNano int64
	if !view.e.IsZero() {
		expireNano = view.Expire().UnixNano()
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.GetResponse{Value: b, Expire: &expireNano})
	if err != nil {
		p.opts.ServerErrorHandler(ctx, w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(body)
}

func (p *HTTPPool) setIncomingRemoteLoadMetadataOnContext(ctx context.Context, rh http.Header, group, key string) context.Context {
	m := &IncomingRemoteLoadMetadata{
		IsRemoteLoad: true,

		Group: group,
		Key:   key,

		RecursiveRemoteLoadAllowed: p.opts.AllowRecursiveRemoteLoad,
		SourcePeer:                 rh.Get(RemoteLoadSourcePeerHeader),
	}
	return context.WithValue(ctx, remoteLoadMetadataContextKey{}, m)
}

func IncomingRemoteLoadMetadataFromContext(ctx context.Context) *IncomingRemoteLoadMetadata {
	if ctx == nil {
		return &IncomingRemoteLoadMetadata{}
	}
	m, ok := ctx.Value(remoteLoadMetadataContextKey{}).(*IncomingRemoteLoadMetadata)
	if !ok {
		return &IncomingRemoteLoadMetadata{}
	}
	return m
}

type httpGetter struct {
	getTransport func(context.Context) http.RoundTripper
	baseURL      string
	// The address of the local node, that needs to be included in the headers for outgoing requests so that the receiving node can detect recursive remote loads and avoid looping.
	selfAddr string
}

// GetURL
func (h *httpGetter) GetURL() string {
	return h.baseURL
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func (h *httpGetter) makeRequest(ctx context.Context, method string, in *pb.GetRequest, out *http.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.PathEscape(in.GetGroup()),
		url.PathEscape(in.GetKey()),
	)

	// Pass along the context to the RoundTripper
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return err
	}

	// Set self's URL in the header so that the destination node can detect recursive remote calls/calls back to self
	req.Header.Set(RemoteLoadSourcePeerHeader, h.selfAddr)

	tr := http.DefaultTransport
	if h.getTransport != nil {
		tr = h.getTransport(ctx)
	}

	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	*out = *res
	return nil
}

func (h *httpGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, in, &res); err != nil {
		errType := fmt.Sprintf("%T", err)
		if et := res.Header.Get(ErrorTypeHeader); et != "" {
			errType = et
		}
		return newRemoteLoadError(in, errType, err)
	}
	defer res.Body.Close()

	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err := io.Copy(b, res.Body)
	if res.StatusCode != http.StatusOK {
		return newRemoteLoadErrorWithResp(in, res, b.Bytes(), res.Header.Get(ErrorTypeHeader), errors.Errorf("non-OK response code: %d %s", res.StatusCode, res.Status))
	}
	if err != nil {
		return newRemoteLoadErrorWithResp(in, res, nil, res.Header.Get(ErrorTypeHeader), errors.Wrapf(err, "reading response body"))
	}

	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return newRemoteLoadErrorWithResp(in, res, b.Bytes(), res.Header.Get(ErrorTypeHeader), errors.Wrapf(err, "decoding response body"))
	}
	return nil
}

func (h *httpGetter) Remove(ctx context.Context, in *pb.GetRequest) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodDelete, in, &res); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("while reading body response: %v", res.Status)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}

func DefaultServerErrorHandler(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {

	if logger != nil {
		logger.WithError(err).Debugf("error while retrieving cache entry for request %q", r.URL)
	}

	w.Header().Set(ErrorTypeHeader, fmt.Sprintf("%T", err))

	switch err.(type) {
	case RecursiveRemoteLoadForbiddenError, BadGroupcacheRequestError:
		http.Error(w, err.Error(), http.StatusBadRequest)
	case GroupNotFoundError:
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}

func (e BadGroupcacheRequestError) Error() string {
	return e.message
}

func (e GroupNotFoundError) Error() string {
	return fmt.Sprintf("group not found: %q", e.group)
}

func newRemoteLoadError(get *pb.GetRequest, errorType string, err error) RemoteLoadError {
	return RemoteLoadError{
		Group: get.GetGroup(),
		Key:   get.GetKey(),

		ErrorType: errorType,
		Err:       err,
	}
}

func newRemoteLoadErrorWithResp(get *pb.GetRequest, resp http.Response, body []byte, errorType string, err error) RemoteLoadError {
	return RemoteLoadError{
		Group: get.GetGroup(),
		Key:   get.GetKey(),

		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Body:       body,

		ErrorType: errorType,
		Err:       err,
	}
}

func (r RemoteLoadError) Error() string {
	return fmt.Sprintf("remote load error: %v", r.Err)
}

func (r RemoteLoadError) Unwrap() error {
	return r.Err
}

func newRecursiveRemoteLoadForbiddenError(group, key, sourcePeer, destinationPeer string, recursiveRemoteLoadAllowed bool) RecursiveRemoteLoadForbiddenError {
	return RecursiveRemoteLoadForbiddenError{
		group: group,
		key:   key,

		sourcePeer:                sourcePeer,
		determinedDestinationPeer: destinationPeer,

		recursiveRemoteLoadAllowed: recursiveRemoteLoadAllowed,
	}
}

func (r RecursiveRemoteLoadForbiddenError) Error() string {
	return fmt.Sprintf("the current node does not own the requested cache key (group:%s, key:%s), "+
		"and the current request is already being handled as part of a remote load. This node either does not allow recursive remote loads (recursiveRemoteLoadAllowed: %t), "+
		"or the destination node (%q) has been identified as being the same as the source node (%q) for the same cache group and key. Please compute the response on the source node",
		r.group, r.key, r.recursiveRemoteLoadAllowed, r.determinedDestinationPeer, r.sourcePeer)
}
