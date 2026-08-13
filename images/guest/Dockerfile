# Builds the guest kernel and initramfs that QEMU boots as a microVM.
FROM alpine:3.21 AS build

RUN apk add --no-cache linux-virt busybox-static cpio gzip

WORKDIR /build

# The init the kernel runs as PID 1.
RUN mkdir -p rootfs/bin rootfs/dev rootfs/proc rootfs/sys && \
    cp /bin/busybox.static rootfs/bin/busybox && \
    for applet in sh mount uname cat sleep echo; do \
      ln -sf busybox rootfs/bin/$applet; \
    done && \
    printf '%s\n' \
      '#!/bin/sh' \
      'mount -t proc none /proc 2>/dev/null' \
      'mount -t sysfs none /sys 2>/dev/null' \
      'echo "MICROVM_READY"' \
      'echo "guest kernel: $(uname -r)"' \
      'echo "guest pid1:   $$"' \
      '# Halting would end the microVM. It stays up until the supervisor stops' \
      '# it or its TTL elapses, which is what makes the slot meaningful.' \
      'while true; do sleep 3600; done' \
      > rootfs/init && \
    chmod +x rootfs/init

RUN mkdir -p /out && \
    (cd rootfs && find . | cpio -o -H newc 2>/dev/null | gzip -9 > /out/initramfs) && \
    cp /boot/vmlinuz-virt /out/vmlinuz && \
    ls -la /out

# Scratch stage so the artifacts can be extracted with `docker build -o`.
FROM scratch AS export
COPY --from=build /out/ /
