#!/bin/bash
: ${PYTHON=python3}

: ${SEAHUB_TEST_USERNAME="test@seafiletest.com"}
: ${SEAHUB_TEST_PASSWORD="testtest"}
: ${SEAHUB_TEST_ADMIN_USERNAME="admin@seafiletest.com"}
: ${SEAHUB_TEST_ADMIN_PASSWORD="adminadmin"}

export SEAHUB_TEST_USERNAME
export SEAHUB_TEST_PASSWORD
export SEAHUB_TEST_ADMIN_USERNAME
export SEAHUB_TEST_ADMIN_PASSWORD

# If you run this script on your local machine, you must set CCNET_CONF_DIR
# and SEAFILE_CONF_DIR like this:
#
#       export CCNET_CONF_DIR=/your/path/to/ccnet
#       export SEAFILE_CONF_DIR=/your/path/to/seafile-data
#

set -e
if [[ ${TRAVIS} != "" ]]; then
    set -x
fi

set -x
EVENTS_TESTDIR=$(python -c "import os; print(os.path.dirname(os.path.realpath('$0')))")
EVENTS_SRCDIR=$(dirname $(dirname "${EVENTS_TESTDIR}"))
HOME_DIR=$(dirname "${EVENTS_SRCDIR}")

export SEAHUB_LOG_DIR='/tmp/logs'
export PYTHONPATH="/usr/local/lib/python3.8/site-packages:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3.8/site-packages:/usr/lib/python3.8/dist-packages:${PYTHONPATH}"
cd "$EVENTS_SRCDIR"
set +x

# Not need to init plugins repo
# # init plugins repo
# repo_id=$(python -c "from seaserv import seafile_api; repo_id = seafile_api.create_repo('plugins repo', 'plugins repo', 'dtable@seafile'); print(repo_id)")
# sudo echo -e "\nPLUGINS_REPO_ID='"${repo_id}"'" >>./seahub/settings.py

function init() {
    # Not need to create user now
    # ###############################
    # # create two new users: an admin, and a normal user
    # ###############################
    # # create normal user
    # $PYTHON -c "import os; from seaserv import ccnet_api; ccnet_api.add_emailuser('${SEAHUB_TEST_USERNAME}', '${SEAHUB_TEST_PASSWORD}', 0, 1);"
    # # create admin
    # $PYTHON -c "import os; from seaserv import ccnet_api; ccnet_api.add_emailuser('${SEAHUB_TEST_ADMIN_USERNAME}', '${SEAHUB_TEST_ADMIN_PASSWORD}', 1, 1);"

    if [ -d $HOME_DIR/dtable_web ]; then
        echo 'dtable-web exists'
    # a fake dtable-web
    else
        mkdir -p $HOME_DIR/dtable_web/seahub
        touch $HOME_DIR/dtable_web/seahub/settings.py
        touch $HOME_DIR/dtable_web/seahub/__init__.py
    fi
}

function set_env() {
    export DTABLE_WEB_DIR=$HOME_DIR/dtable_web
}

function run_tests() {
    set_env
    set -e
    # test sql
    python ${EVENTS_TESTDIR}/sql/sql_test.py
}

case $1 in
    "init")
        init
        ;;
    "test")
        run_tests
        ;;
    *)
        echo "unknow command \"$1\""
        ;;
esac
