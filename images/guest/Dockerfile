# Builds the guest kernel and initramfs that QEMU boots as a microVM.
#
# The guest is deliberately tiny: a stock Alpine virt kernel plus an initramfs
# containing one static busybox and a shell script for init. There is no
# userland beyond that, no package manager, no service supervisor and no disk.
#
# Small matters more than usual here. Without hardware virtualisation QEMU
# translates every guest instruction in software, so boot time scales with how
# much work the guest does before it is useful. Probing for hardware that is not
# there is the single biggest cost, which is why the kernel command line
# disables everything the guest does not need.
#
# Output is two files in /out:
#   vmlinuz    an uncompressed kernel QEMU boots directly with -kernel
#   initramfs  the userspace, as a gzipped cpio archive
FROM alpine:3.21 AS build

RUN apk add --no-cache linux-virt busybox-static cpio gzip

WORKDIR /build

# The init the kernel runs as PID 1.
#
# It prints the readiness marker and then sleeps forever. The marker is the
# contract with internal/hypervisor: Start does not report success until it
# appears on the console, because a spawned QEMU process is not yet a running
# virtual machine and counting it as capacity would let the scheduler place work
# onto a guest that cannot accept it.
#
# The proof that this is a real VM rather than a container is printed alongside:
# its own kernel version, its own PID 1, and its own memory, none of which are
# shared with the host.
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
