package rtsp

import (
	"regexp"
	"sync"
)

// liveNameRe restricts published stream names to 1-32 ASCII letters,
// digits or underscores.
var liveNameRe = regexp.MustCompile("^[A-Za-z0-9_]{1,32}$")

func validLiveName(name string) bool { return liveNameRe.MatchString(name) }

// registry owns the set of live sources by name. A name is held by at
// most one publisher at a time.
type registry struct {
	mu      sync.Mutex
	sources map[string]*liveSource
}

func newRegistry() *registry {
	return &registry{sources: map[string]*liveSource{}}
}

// register claims name for src. Returns false when the name is taken.
func (r *registry) register(name string, src *liveSource) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sources[name]; ok {
		return false
	}
	r.sources[name] = src
	return true
}

// unregister releases name only if it is still held by src, so cleanup
// of a stale publisher never removes a newer source with the same name.
func (r *registry) unregister(name string, src *liveSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.sources[name]; ok && cur == src {
		delete(r.sources, name)
	}
}

func (r *registry) get(name string) *liveSource {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sources[name]
}
