import glob
import logging
import os
import hashlib
import datetime
import json
import shutil

import bagit
from sword2 import Connection

class SwordIngester:
    def __init__(self, sd_iri, username, password):
        self.sd_iri = sd_iri
        self.username = username
        self.password = password
        sword_conn = Connection(self.sd_iri, self.username, self.password)
        if sword_conn is None:
            raise ValueError("SWORD Error: Invalid value in one or more parameters: SD IRI, username, password.")
        self.connection = sword_conn
    def ingest(self, bagit_file):
        # pick the first collection within the first workspace:
        # workspace_1_title, workspace_1_collections = self.connection.workspaces[0]
        # collection = workspace_1_collections[0]
        # logging.debug(collection)
        # logging.debug(collection.href)
        # print(f'collection.href: {collection.href}')
        # logging.debug(f'bagit_file: {bagit_file}')
        # logging.debug(f'os.path.basename(bagit_file): {os.path.basename(bagit_file)}')
        print(f'self.sd_iri: {self.sd_iri}')
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



