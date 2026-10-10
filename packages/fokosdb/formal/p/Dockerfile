FROM mcr.microsoft.com/dotnet/sdk:8.0

ARG P_VERSION=3.1.0

# PEx compiles the model to Java and builds it with Maven.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless maven \
    && rm -rf /var/lib/apt/lists/*

ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    PATH="${PATH}:/opt/p"

RUN dotnet tool install --tool-path /opt/p P --version "${P_VERSION}" && p --version

WORKDIR /workspace
