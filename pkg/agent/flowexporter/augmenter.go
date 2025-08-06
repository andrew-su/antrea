package flowexporter

import (
	"antrea.io/antrea/pkg/agent/proxy"
	"antrea.io/antrea/pkg/util/objectstore"
)

type DefaultAugmenter struct {
	podStore      objectstore.PodStore
	antreaProxier proxy.Proxier
}
