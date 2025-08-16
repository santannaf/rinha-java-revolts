FROM alpine:3.19

RUN apk add --no-cache libc6-compat

WORKDIR /app

COPY ./build/native/nativeCompile/rinha ./app/rinha

RUN chmod +x ./app/rinha

EXPOSE 8080

ENTRYPOINT ["./app/rinha"]