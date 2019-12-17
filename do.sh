#!/bin/bash

COLUMNS="`tput cols`"
LINES="`tput lines`"
BOLD=$(tput bold)
NORMAL=$(tput sgr0)
HUID=`id -u`
DOCKER_COMPOSE="docker-compose -f docker-compose.yml"

if [[ -e "docker-compose.override.yml" ]]; then
    # This file is local/optional and NOT checked in.
    DOCKER_COMPOSE="$DOCKER_COMPOSE -f docker-compose.override.yml"
fi

os=${OSTYPE//[0-9.-]*/}
case "$os" in
  darwin)
    ;;

  msys)
    DOCKER_COMPOSE="winpty $DOCKER_COMPOSE"
    ;;

  linux)
    ;;
  *)

  echo "Unsupported operating system $OSTYPE"
  exit 1
esac

_requires() {
    service="$1"
    $DOCKER_COMPOSE ps -q $service &> /dev/null
    if [[ "$?" == 1 ]]; then
        echo "'$service' service is not running. Please run \`start\` first."
        exit 1
    fi
}

build() {
    $DOCKER_COMPOSE build --force-rm --build-arg huid=${HUID} "$@"
}

compose() {
    $DOCKER_COMPOSE "$@"
}

init() {
   echo Setting up pre-commit hooks
   run python pre-commit install
   mv .git/hooks/pre-commit .git/hooks/pre-commit-docker.py
   chmod -x .git/hooks/pre-commit-docker.py
   cat << EOF >> .git/hooks/pre-commit
#!/bin/sh
DOPATH=$( cd "$(dirname "$0")"; cd ../..; pwd -P )
./do.sh run python python /usr/src/app/.git/hooks/pre-commit-docker.py 
EOF
  chmod +x .git/hooks/pre-commit
  run python pre-commit install-hooks
}


test() {
   run -e PYTHONPATH=/usr/src/app python py.test "$@" 
}

lint() {
   run python flake8 "$@"
}

shell() {
    run $@ /bin/bash
}

pyshell() {
    run $@ python ipython
}

exec() {
    $DOCKER_COMPOSE exec -e COLUMNS -e LINES "$@"
}

run() {
    $DOCKER_COMPOSE run --rm -e HUID=$HUID -w //usr/src/app "$@"
}

serverless() {
   run serverless serverless "$@"
}

_usage() {
    cat <<USAGE
Utility wrapper around docker-compose.

Usage:

    ${BOLD}build${NORMAL} [<arg>]

        Builds all the images (or the ones specified).

        Ex:
            ./do.sh build

    ${BOLD}run${NORMAL} [<arg>]

        Bring up a container, execute a command in the container, removes the container after the command has finished.

    ${BOLD}check${NORMAL}

        Checks python code and dependencies for issues.

    ${BOLD}shell${NORMAL}
        Brings up a container, runs a shell, removes container once shell exits.

    ${BOLD}pyshell${NORMAL}

        Brings up a container, opens an ipython shell, removed container once shell exits.

    ${BOLD}test${NORMAL}

        Brings up python container, run backend tests using pytest, container removed once tests have finished. 

USAGE
}

if [ "$1" == "" ]; then
    _usage
fi

$*
