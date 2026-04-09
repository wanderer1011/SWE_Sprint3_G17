FROM clickhouse/clickhouse-server:24.3
COPY schemas/ /docker-entrypoint-initdb.d/
COPY config.xml /etc/clickhouse-server/config.d/custom.xml
COPY users.xml /etc/clickhouse-server/users.d/custom.xml
EXPOSE 8123 9000 9363                    
