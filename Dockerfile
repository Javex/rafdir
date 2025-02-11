FROM creativeprojects/resticprofile:0.29.0
RUN apk add kubectl
RUN mkdir -p /etc/restic/profiles.d
