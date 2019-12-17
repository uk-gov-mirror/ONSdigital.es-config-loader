#!/bin/sh
set -e

CURRENT_UID=${uid:-9999}
# Notify user about the UID selected
echo "Current UID : $CURRENT_UID"

# Create user called "docker" with selected UID
useradd --shell /bin/bash -u $CURRENT_UID -o -c "" -m dockeruser

# Set "HOME" ENV variable for user's home directory
export HOME=/home/dockeruser
 
# Execute process
exec /usr/sbin/gosu $CURRENT_UID "$@"
gosu docker "$@"
