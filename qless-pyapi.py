import json
import qless
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound


class QlessPyapi(object):
    def __init__(self, qless_client):
        self.client = qless_client
        self.url_map = Map([
            Rule('/', endpoint='root'),
            Rule('/queues', endpoint='queues'),
            Rule('/queues/<queue_name>', endpoint='queues_get'),
            Rule('/queues/<queue_name>/stats', endpoint='queues_stats'),
        ])

    def on_root(self, request):
        return Response('Hello World!')

    def on_queues(self, request):
        queues = self.client.queues.counts
        return Response(json.dumps(queues), content_type='application/json')

    def on_queues_get(self, request, queue_name):
        queue = self.client.queues[queue_name].counts
        return Response(json.dumps(queue), content_type='application/json')

    def on_queues_stats(self, request, queue_name):
        queue = self.client.queues[queue_name].stats()
        return Response(json.dumps(queue), content_type='application/json')

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except HTTPException, e:
            return e

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)


def create_app(redisurl='redis://localhost'):
    client = qless.Client(redisurl)
    return QlessPyapi(client)


if __name__ == '__main__':
    from werkzeug.serving import run_simple

    app = create_app()
    run_simple('0.0.0.0', 4000, app)
    # reloader_type='watchdog'
