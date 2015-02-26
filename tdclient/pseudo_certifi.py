#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import os

def where():
    certs = [
        "/etc/ssl/certs/ca-certificates.crt", # debian
        "/etc/ssl/certs/ca-bundle.crt", # redhat
    ]
    for cert in certs:
        if os.path.exists(cert):
            return cert
    return None
