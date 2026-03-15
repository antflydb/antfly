package multirafthttp

import (
	"fmt"
	"net/http"

	"github.com/Masterminds/semver"
)

const (
	MinClusterVersion = "0.0.0-dev0"
	Version           = "0.0.1"
)

var (
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")

	supportedStream = map[streamType]string{
		streamTypeMsgAppV2: ">= 0.0.0-dev0",
		streamTypeMessage:  ">= 0.0.0-dev0",
	}
)

// checkStreamSupport checks whether the stream type is supported in the
// given version.
func checkStreamSupport(v *semver.Version, t streamType) bool {
	constraint, _ := semver.NewConstraint(supportedStream[t])
	return constraint.Check(v)
}

// serverVersion returns the server version from the given header.
func serverVersion(h http.Header) *semver.Version {
	verStr := h.Get("X-Server-Version")
	if verStr == "" {
		verStr = "0.0.0"
	}
	return semver.MustParse(verStr)
}

// minClusterVersion returns the min cluster version from the given header.
func minClusterVersion(h http.Header) *semver.Version {
	verStr := h.Get("X-Min-Cluster-Version")
	if verStr == "" {
		verStr = "0.0.0-dev0"
	}
	return semver.MustParse(verStr)
}

// checkVersionCompatibility checks whether the given version is compatible
// with the local version.
func checkVersionCompatibility(name string, server, minCluster *semver.Version) (
	localServer *semver.Version,
	localMinCluster *semver.Version,
	err error,
) {
	localServer = semver.MustParse(Version)
	localMinCluster = semver.MustParse(MinClusterVersion)

	if server.Compare(localMinCluster) == -1 {
		return localServer, localMinCluster, fmt.Errorf("remote version is too low: remote[%s]=%s, local=%s", name, server, localServer)
	}
	if localServer.Compare(minCluster) == -1 {
		return localServer, localMinCluster, fmt.Errorf("local version is too low: remote[%s]=%s, local=%s", name, server, localServer)
	}
	return localServer, localMinCluster, nil
}
