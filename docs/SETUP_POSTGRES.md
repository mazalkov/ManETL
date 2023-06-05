### Short Instructions to setup postresql@14

# Mac Setup
1. if you haven't already; setup brew
```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

2. locate the postresql download in brew online (command given)
```shell
brew install postgresql@14
```

3. run the `.setup_postgres.sh` script

4. to enter the db instance run
```shell
psql -d postgres
```

### Optional
4. connect to the database
- Database server: localhost
- Database port: 5432
- Database userid: your username 

to get this information run:
```shell
psql --help | grep username
psql --help | grep host
psql --help | grep port
```
