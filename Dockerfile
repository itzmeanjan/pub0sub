FROM golang:alpine as builder
RUN apk add --no-cache make gcc musl-dev linux-headers git
WORKDIR /app
COPY . .
RUN cd cli/hub; go build -o ../../0hub; cd ../..

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/0hub /usr/bin
EXPOSE 13000
ENTRYPOINT [ "0hub", "-addr", "0.0.0.0", "-port", "13000", "-capacity", "4096" ]
