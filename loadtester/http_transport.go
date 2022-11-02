package loadtester

import (
	"net"
	"net/http"
	"runtime"
	"time"
)

// HttpTransport returns a new configured *http.Transport
// which implements http.RoundTripper that can be used in
// tasks which have http clients
//
// Note, you may need to increase the value of MaxIdleConns
// if your tasks target multiple hosts. MaxIdleConnsPerHost
// does not override the limit established by MaxIdleConns
// and if the tasks are expected to communicate to multiple
// hosts you probably need to apply some scaling factor to
// it to let connections go idle for a time and still be reusable.
//
// Note that if you are not connecting to a load balancer
// which preserves connections to a client much of the intent
// we're trying to establish here is not applicable.
//
// Also if the load balancer does not have "max connection lifespan"
// behavior nor a "round robin" or "connection balancing" feature
// without forcing the loadtesting client to reconnect then as
// you increase load your established connections may prevent the
// spread of load to newly scaled-out recipients of that load.
//
// By default golang's http standard lib does not expose a way for us
// to attempt to address this. The problem is also worse if your
// load balancer ( or number of exposed ips for a dns record ) increase.
//
// Rectifying this issue requires a fix like/option like
// https://github.com/golang/go/pull/46714
// to be accepted by the go maintainers.
func (lt *Loadtest) NewHttpTransport() *http.Transport {

	// adding runtime CPU count to the max
	// just to ensure whenever one worker releases
	// a connection back to the pool we're not impacted
	// by the delay of that connection getting re-pooled
	// it the underlying implementation which makes it available
	// for use again is asynchronous
	maxConnections := lt.maxWorkers + runtime.NumCPU()

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConnections,
		IdleConnTimeout:       20 * time.Second, // default was 90
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   maxConnections,
		MaxConnsPerHost:       maxConnections,
	}
}
