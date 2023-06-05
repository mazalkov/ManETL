# if the db doesnt exist
if [ ! -d "postgres" ]; then
  mkdir -p postgres
  # init db
  pg_ctl initdb -D "${PWD}/postgres"
fi
# set default postgres database name
export PGDATABASE=postgres
export PGDATA=postgres

#start the server at this filepath
if ! pg_isready &>/dev/null; then
  echo "starting posgres server"
  pg_ctl start;
else
  echo "server already started"
fi



