#!/usr/bin/env python

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
