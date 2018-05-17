FROM python:2.7-slim

ENV SOLR_HOST localhost:8983
ENV BACKUP_NAME backup
ENV BACKUP_PATH /opt/solr-backups/
ENV MANIFEST_DIR /manifests

ADD requirements.txt /
RUN pip install -r requirements.txt

ADD solr_backups.py /
ADD entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

CMD ["--backup"]