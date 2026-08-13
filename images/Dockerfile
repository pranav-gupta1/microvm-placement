# Single Dockerfile for all three binaries, selected by the BINARY build arg.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first so a source-only change does not re-download the module
# graph on every build.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG BINARY
ARG TARGETARCH
# CGO off for a static binary that runs on a distroless base. -trimpath keeps
# build paths out of the binary so images are reproducible.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${BINARY}

# vmhostd needs a real QEMU to boot real guests, so it cannot use a distroless
# base.
FROM alpine:3.21 AS vmhostd
# qemu-system-aarch64 provides the emulator; the guest kernel and initramfs are
# built separately and mounted in.
RUN apk add --no-cache qemu-system-aarch64 qemu-system-x86_64 || \
    apk add --no-cache qemu-system-aarch64
COPY --from=build /out/app /usr/local/bin/app
# Guest kernel and initramfs, if they have been built.
COPY images/guest/build/ /guest/
# Non-root: QEMU needs no privilege for software emulation, and only needs
# access to /dev/kvm when hardware acceleration is available.
RUN adduser -D -u 65532 nonroot
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/app"]

# Default stage: a distroless base for the pure-Go services.
FROM gcr.io/distroless/static-debian12:nonroot AS service
COPY --from=build /out/app /usr/local/bin/app
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/app"]
