# Builds the QEMU guest kernel and initramfs.

BUILD   := build
DOCKER  ?= docker
PLATFORM ?= linux/arm64

.DEFAULT_GOAL := all

# # all: build the guest kernel and initramfs into build/.
.PHONY: all
all: $(BUILD)/vmlinuz

$(BUILD)/vmlinuz: Dockerfile
	@mkdir -p $(BUILD)
	DOCKER_BUILDKIT=1 $(DOCKER) build \
		--platform=$(PLATFORM) \
		--target=export \
		--output=type=local,dest=$(BUILD) \
		-f Dockerfile .
	@ls -lh $(BUILD)

# # clean: remove built artifacts.
.PHONY: clean
clean:
	rm -rf $(BUILD)/vmlinuz $(BUILD)/initramfs
