compile:
	go mod download
	CGO_ENABLED=0 go build -tags static -v -a -installsuffix cgo
