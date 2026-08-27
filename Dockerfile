FROM golang:1.25-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG SERVICE=api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/eventmesh ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=build /out/eventmesh /eventmesh

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/eventmesh"]
