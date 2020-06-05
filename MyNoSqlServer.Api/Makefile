NS = amigin
VERSION ?= 1.0.9

REPO = mynosqlserver

build:
	docker build -t $(NS)/$(REPO):$(VERSION) .

push:
	docker push $(NS)/$(REPO):$(VERSION)
