FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /payd ./cmd/payd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /payd /payd
EXPOSE 8080
ENTRYPOINT ["/payd"]
