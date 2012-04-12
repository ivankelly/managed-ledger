#!/bin/bash

CURRENT=$(date -u '+%F %T %Z')

if [ -z ${VERSION} ]; then
    echo "ERROR: No version number defined";
    exit 1;
fi

cat <<EOF

The Managed-Ledger package.

JavaDocs: 
http://java.corp.yahoo.com/javadocs/data/managed-ledger/${VERSION}/managed-ledger.jar/javadoc

(The above link will not work until ${VERSION} of package is promoted off of 
quarantine)

ChangeLog:

Version ${VERSION} (${CURRENT})
  * [BUG 1234567890] initial version. 

