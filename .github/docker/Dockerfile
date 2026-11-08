# syntax=docker/dockerfile:1
ARG GO_VERSION
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build

# Build xray-core
WORKDIR /src
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
ARG BUILD_COMMIT
RUN if [ "$TARGETARCH" = arm ]; then export GOARM="${TARGETVARIANT#v}"; fi; \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 GOTOOLCHAIN=local \
    go build -o xray -trimpath -buildvcs=false -gcflags="all=-l=4" \
    -ldflags "-s -w -buildid= -X github.com/xtls/xray-core/core.build=$(printf '%.7s' "$BUILD_COMMIT")" ./main

# Use the same resource snapshot as the release archives.
COPY resources/docker/geoip.dat resources/docker/geosite.dat /tmp/geodat/

RUN mkdir -p /tmp/empty

# Create config files with empty JSON content
RUN mkdir -p /tmp/usr/local/etc/xray
RUN cat <<EOF >/tmp/usr/local/etc/xray/00_log.json
{
  "log": {
    "error": "/var/log/xray/error.log",
    "loglevel": "warning",
    "access": "none",
    "dnsLog": false
  }
}
EOF
RUN echo '{}' >/tmp/usr/local/etc/xray/01_api.json
RUN echo '{}' >/tmp/usr/local/etc/xray/02_dns.json
RUN echo '{}' >/tmp/usr/local/etc/xray/03_routing.json
RUN echo '{}' >/tmp/usr/local/etc/xray/04_policy.json
RUN echo '{}' >/tmp/usr/local/etc/xray/05_inbounds.json
RUN echo '{}' >/tmp/usr/local/etc/xray/06_outbounds.json
RUN echo '{}' >/tmp/usr/local/etc/xray/07_stats.json
RUN echo '{}' >/tmp/usr/local/etc/xray/08_fakedns.json
RUN echo '{}' >/tmp/usr/local/etc/xray/09_metrics.json
RUN echo '{}' >/tmp/usr/local/etc/xray/10_observatory.json
RUN echo '{}' >/tmp/usr/local/etc/xray/11_geodata.json
RUN echo '{}' >/tmp/usr/local/etc/xray/12_env.json
RUN echo '{}' >/tmp/usr/local/etc/xray/99_version.json

# Build finally image
FROM gcr.io/distroless/static:nonroot

ARG BUILD_COMMIT
ARG VERSION
ARG SOURCE
LABEL org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$BUILD_COMMIT \
      org.opencontainers.image.source=$SOURCE

COPY --from=build --chown=0:0 --chmod=755 /src/xray /usr/local/bin/xray
COPY --from=build --chown=65532:65532 --chmod=755 /tmp/empty /usr/local/share/xray
COPY --from=build --chown=65532:65532 --chmod=644 /tmp/geodat/*.dat /usr/local/share/xray/
COPY --from=build --chown=0:65532 --chmod=750 /tmp/empty /usr/local/etc/xray
COPY --from=build --chown=0:65532 --chmod=640 /tmp/usr/local/etc/xray/*.json /usr/local/etc/xray/
COPY --from=build --chown=65532:65532 --chmod=700 /tmp/empty /var/log/xray

VOLUME /usr/local/etc/xray
VOLUME /var/log/xray

ARG TZ=Etc/UTC
ENV TZ=$TZ

ENTRYPOINT [ "/usr/local/bin/xray" ]
CMD [ "-confdir", "/usr/local/etc/xray/" ]
