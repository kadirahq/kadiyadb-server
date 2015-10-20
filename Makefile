
publish: compile
	docker push kadirahq/kadiyadb-server:latest

compile: clean
	GOOS="linux" GOARCH="amd64" go build -o kadiyadb-server -i -a .
	docker build -t kadirahq/kadiyadb-server ./

clean:
	rm -rf kadiyadb-server
