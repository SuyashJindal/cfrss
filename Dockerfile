FROM golang:1.18-alpine AS builder

WORKDIR /build

ENV GOPROXY https://goproxy.io

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/

RUN CGO_ENABLED=0 GOOS=linux go build -a -o app cmd/web/main.go

FROM node:18.4.0 as frontend-builder
WORKDIR /frontend-assets
COPY frontend/package.json .
COPY frontend/package-lock.json .
RUN npm install --only=prod
COPY frontend/ .
RUN npm run build

FROM alpine:3.16

WORKDIR /cfrss
COPY --from=builder /build/app bin/app
COPY --from=frontend-builder /frontend-assets/build frontend/build

ENTRYPOINT [ "bin/app" ]