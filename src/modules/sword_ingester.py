import logging
import os

from sword2 import Connection

from src.commons import settings, dmz_headers


class SwordIngester:
    def __init__(self, sd_iri, username, password):
        self.sd_iri = sd_iri
        self.username = username
        self.password = password
        ca_certs = None
        if settings.exists("ca_certs_file", fresh=False):
            ca_certs = settings.CA_CERTS_FILE

        sword_conn = Connection(self.sd_iri, self.username, self.password, ca_certs=ca_certs, headers=dmz_headers(username, password))
        if sword_conn is None:
            raise ValueError("SWORD Error: Invalid value in one or more parameters: SD IRI, username, password.")
        self.connection = sword_conn
    def ingest(self, bagit_file):
        logging.debug(f'self.sd_iri: {self.sd_iri}')
        logging.debug(f'bagit_file: {bagit_file}')
        with open(bagit_file,
                  "rb") as pkg:
            receipt = self.connection.create(col_iri=self.sd_iri,
                               payload=pkg,
                               mimetype="application/zip",
                               filename=os.path.basename(bagit_file),
                               packaging='http://purl.org/net/sword/package/BagIt',
                               in_progress=False)  # As the deposit isn't yet finished
            logging.debug(receipt.links['http://purl.org/net/sword/terms/statement'][0]['href'])
            return receipt.links['http://purl.org/net/sword/terms/statement'][0]['href']



