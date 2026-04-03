MAKE_DIR:=$(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ROOT_DIR:=$(abspath $(MAKE_DIR))

clean:
	@echo "Cleaning generated files..."
	@rm $(ROOT_DIR)/groupcachepb/groupcache.pb.go
	@rm $(ROOT_DIR)/testpb/test.pb.go

generate-all:
	@echo "Generating all files..."
	@protoc --go_out=$(ROOT_DIR)/groupcachepb --proto_path $(ROOT_DIR)/groupcachepb --go_opt=paths=source_relative $(ROOT_DIR)/groupcachepb/groupcache.proto
	@protoc --go_out=$(ROOT_DIR)/groupcachepb --proto_path $(ROOT_DIR)/groupcachepb --go_opt=paths=source_relative $(ROOT_DIR)/groupcachepb/example.proto
	@protoc --go_out=$(ROOT_DIR)/testpb --proto_path $(ROOT_DIR)/testpb --go_opt=paths=source_relative $(ROOT_DIR)/testpb/test.proto
