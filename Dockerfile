FROM creativeprojects/resticprofile:0.29.0 AS resticprofile

FROM alpine:latest AS builder
RUN apk add --no-cache git go

WORKDIR /build
COPY go.mod go.sum /build/
RUN go mod download
COPY internal /build/internal
COPY cli /build/cli
COPY resticprofile_k8s.go /build/
RUN go build -o ./cli/resticprofile-kubernetes ./cli/resticprofile-kubernetes.go

FROM alpine:latest

LABEL org.opencontainers.image.source="https://github.com/Javex/resticprofile-kubernetes"

RUN apk add --no-cache ca-certificates curl logrotate openssh-client-default tzdata kubectl
RUN mkdir -p /etc/restic/profiles.d

ARG ARCH=amd64
ENV TZ=Etc/UTC

COPY --from=resticprofile /usr/bin/restic /usr/bin/restic
COPY --from=resticprofile /usr/bin/resticprofile /usr/bin/resticprofile
COPY --from=builder /build/cli/resticprofile-kubernetes /usr/bin/resticprofile-kubernetes

ENTRYPOINT ["resticprofile-kubernetes"]
