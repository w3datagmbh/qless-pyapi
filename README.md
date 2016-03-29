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
$EDITOR config.json
```

`hostname` hostname / ip to bind
`port` to bind
`ui` if true - enable qless-ui on `/app` and move api to `/api`, else only api on `/`
`redis` redis connection string (url)
`groups` configure queue groups, uses regex for matching (`$` - not matched by other groups)