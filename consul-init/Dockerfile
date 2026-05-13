FROM alpine:3.19
RUN apk add --no-cache curl
COPY init.sh /init.sh
RUN chmod +x /init.sh
CMD ["/init.sh"]
