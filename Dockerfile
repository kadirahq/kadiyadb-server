FROM alpine:3.2

COPY build/linux/kadiyadb-server kadiyadb-server
CMD ["kadiyadb-server", "-data=/data", "-addr=:8000"]

EXPOSE 6060
EXPOSE 8000

VOLUME ["/data"]
