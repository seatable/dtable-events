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

export SEAHUB_LOG_DIR='/tmp/logs'
export PYTHONPATH="/usr/local/lib/python3.8/site-packages:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3.8/site-packages:/usr/lib/python3.8/dist-packages:${PYTHONPATH}"
cd "$EVENTS_SRCDIR"
set +x

# # init plugins repo
# repo_id=$(python -c "from seaserv import seafile_api; repo_id = seafile_api.create_repo('plugins repo', 'plugins repo', 'dtable@seafile'); print(repo_id)")
# sudo echo -e "\nPLUGINS_REPO_ID='"${repo_id}"'" >>./seahub/settings.py

function init() {
    ###############################
    # create two new users: an admin, and a normal user
    ###############################
    # create normal user
    $PYTHON -c "import os; from seaserv import ccnet_api; ccnet_api.add_emailuser('${SEAHUB_TEST_USERNAME}', '${SEAHUB_TEST_PASSWORD}', 0, 1);"
    # create admin
    $PYTHON -c "import os; from seaserv import ccnet_api; ccnet_api.add_emailuser('${SEAHUB_TEST_ADMIN_USERNAME}', '${SEAHUB_TEST_ADMIN_PASSWORD}', 1, 1);"

}

function run_tests() {
    set -e
    # test sql
    which python
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
