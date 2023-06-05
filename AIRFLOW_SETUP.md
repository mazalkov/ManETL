## The following instructions are for settting up airflow

### Specify the directory in which to install airflow
provided you are in the root directory of this project run the following commands in your terminal + virtualenv
(dont do this if you already see `airflow` - try see if the `airflow.cfg` file is overwritten if you do this)
```shell
export AIRFLOW_HOME="${PWD}/airflow"
export AIRFLOW_CONFIG="${AIRFLOW_HOME}/airflow.cfg"
```

### Install Airflow
the following script will set up your airflow environment
```shell
AIRFLOW_VERSION=2.6.1

# Extract the version of Python you have installed. If you're currently using Python 3.11 you may want to set this manually as noted above, Python 3.11 is not yet supported.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.6.1 with python 3.7: https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.7.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" --no-cache-dir
```

### Utils
if anyone can get the `.setup_airflow.sh` script to work correctly that would be great!

