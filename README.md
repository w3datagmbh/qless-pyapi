# Qless pyAPI

Qless REST API wrapper in python, includes Qless UI.

## Install (Qless UI)

```
git submodule update --init --recursive
cd qless-ui; npm install; cd -
```


## Run Server

```
python2 qless-pyapi.py
```

Now browse to the app at `http://localhost:4000/app/index.html`.


## Config
```
cp config.json.sample config.json
```

`redis` redis connection string (url)
`groups` configure queue groups, uses regex for matching (`$` - not matched by other groups)