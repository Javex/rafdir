FROM creativeprojects/resticprofile:0.29.0 AS resticprofile

FROM alpine:latest

LABEL org.opencontainers.image.source="https://github.com/Javex/resticprofile-kubernetes"

RUN apk add --no-cache ca-certificates curl logrotate openssh-client-default tzdata kubectl
RUN mkdir -p /etc/restic/profiles.d

ARG ARCH=amd64
ENV TZ=Etc/UTC

COPY --from=resticprofile /usr/bin/restic /usr/bin/restic
COPY --from=resticprofile /usr/bin/resticprofile /usr/bin/resticprofile
COPY ./cli/resticprofile-kubernetes /usr/bin/resticprofile-kubernetes

ENTRYPOINT ["resticprofile-kubernetes"]
