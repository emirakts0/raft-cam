package web

import (
	"embed"
	"io/fs"
)

//go:embed dist/*
var distFS embed.FS

// GetDistFS returns the embedded sub-filesystem for the frontend dist directory
func GetDistFS() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic(err)
	}
	return sub
}
