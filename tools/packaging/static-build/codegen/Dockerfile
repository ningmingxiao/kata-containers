# Copyright (c) 2025 Edgeless Systems gmbH
#
# SPDX-License-Identifier: Apache-2.0

# ARG GO_VERSION=1.25.12

FROM docker.1ms.run/busybox:1.37.0 AS resources

WORKDIR /usr/local

ARG PROTOC_VERSION=v21.12
ADD protoc.zip /tmp/
RUN unzip /tmp/protoc.zip

FROM docker.1ms.run/golang:1.25.12

COPY --from=resources /usr/local /usr

ARG PROTOC_GEN_GO_VERSION=v1.28.1
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}
ARG TTRPC_VERSION=v1.2.7
# ENV GOPROXY=https://goproxy.cn

# ENV HTTP_PROXY=http://proxyhk.zte.com.cn:80
# ENV HTTPS_PROXY=http://proxyhk.zte.com.cn:80
# ENV NO_PROXY=${NO_PROXY}
RUN go install github.com/containerd/ttrpc/cmd/protoc-gen-go-ttrpc@${TTRPC_VERSION}
RUN go install github.com/containerd/ttrpc/cmd/protoc-gen-gogottrpc@${TTRPC_VERSION}
