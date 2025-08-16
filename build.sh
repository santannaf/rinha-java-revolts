#!/usr/bin/env bash


./gradlew clean build && ./gradlew nativeCompile && \


docker buildx build --platform linux/amd64 -t thalessantanna/payments-the-revolts:0.0.2 --push .

