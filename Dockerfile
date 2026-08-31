FROM alpine:latest

RUN apk add --no-cache ca-certificates \
    && update-ca-certificates

ARG TARGETPLATFORM
COPY .docker/${TARGETPLATFORM#linux/}/calagopus-tundra /usr/bin/calagopus-tundra

ENV OCI_CONTAINER=official

ENTRYPOINT ["/usr/bin/calagopus-tundra"]
