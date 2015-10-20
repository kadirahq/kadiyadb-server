
compile: clean
	mkdir -p build/{linux,darwin}
	GOOS="linux" GOARCH="amd64" go build -o build/linux/kadiyadb-server -i -a .
	GOOS="darwin" GOARCH="amd64" go build -o build/darwin/kadiyadb-server -i -a .
	docker build -t kadirahq/kadiyadb-server ./

publish: compile
	docker push kadirahq/kadiyadb-server:latest

clean:
	rm -rf build
