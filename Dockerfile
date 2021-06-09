FROM golang:alpine as builder
RUN apk add --no-cache make gcc musl-dev linux-headers git
WORKDIR /app
COPY . .
RUN cd cli/hub; go build -o ../../0hub; cd ../..

FROM alpine:latest
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/0hub /usr/bin
ENV ADDR="0.0.0.0"
ENV PORT=13000
ENV CAPACITY=4096
EXPOSE ${PORT}
ENTRYPOINT [ "0hub" ]
