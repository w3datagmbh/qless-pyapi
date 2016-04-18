# Qless pyAPI

Qless REST API wrapper in python, includes Qless UI.

## Install (Qless UI)

```
sudo apt-get install nodejs-legacy npm
git submodule update --init --recursive
cd qless-ui; npm install; cd -
```

## Install Dependencies (Qless pyAPI)
```
sudo apt-get install python-pip
sudo pip install redis qless-py simplejson werkzeug
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

* `hostname` hostname / ip to bind
* `port` to bind
* `ui` if true - enable qless-ui on `/app` and move api to `/api`, else only api on `/`
* `redis` redis connection string (url)
* `groups` configure queue groups, uses regex for matching (`$` - not matched by other groups)