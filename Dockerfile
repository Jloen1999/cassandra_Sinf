ARG VERSION=latest
FROM cassandra:${version}

ENV CQLSH_HOST=192.168.1.94
ENV CQLSH_PORT=9042

COPY . /usr/src/app

WORKDIR /usr/src/app

