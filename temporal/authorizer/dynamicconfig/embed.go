// Package dynamicconfig carries Kontra's default Temporal dynamic config, so the server
// binary runs with the right features and per-namespace limits even when no file is
// mounted (tests, `kontra dev`).
package dynamicconfig

import _ "embed"

// Default is the content of kontra.yaml.
//
//go:embed kontra.yaml
var Default []byte
