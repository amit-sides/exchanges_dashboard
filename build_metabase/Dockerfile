FROM ubuntu:22.04 AS local-metabase

ENV FC_LANG en-US LC_CTYPE en_US.UTF-8
ARG UID=1000
ARG GID=1000

# dependencies
RUN apt-get update -yq && apt-get install -yq python3 bash fonts-dejavu-core fonts-dejavu-extra fontconfig curl openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    mkdir -p /app/certs && \
    curl https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem -o /app/certs/rds-combined-ca-bundle.pem  && \
    keytool -noprompt -import -trustcacerts -alias aws-rds -file /app/certs/rds-combined-ca-bundle.pem -keystore /etc/ssl/certs/java/cacerts -keypass changeit -storepass changeit && \
    curl https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem -o /app/certs/DigiCertGlobalRootG2.crt.pem  && \
    keytool -noprompt -import -trustcacerts -alias azure-cert -file /app/certs/DigiCertGlobalRootG2.crt.pem -keystore /etc/ssl/certs/java/cacerts -keypass changeit -storepass changeit && \
    mkdir -p /plugins && chmod a+rwx /plugins && \
    groupadd -g ${GID} metabase && useradd --shell /bin/bash -u ${UID} -g ${GID} metabase

WORKDIR /app

# copy app from the official image
COPY --from=metabase/metabase:latest /app /app
RUN mkdir /metabase-data
RUN mkdir /data

RUN chown -R metabase:metabase /app
RUN chown -R metabase:metabase /metabase-data
RUN chown -R metabase:metabase /data

USER metabase

# expose our default runtime port
EXPOSE 3000

# run it
CMD ["/app/run_metabase.sh"]

FROM local-metabase

ADD wrap_metabase.py /app/wrap_metabase.py
CMD ["python3", "-u", "/app/wrap_metabase.py"]
