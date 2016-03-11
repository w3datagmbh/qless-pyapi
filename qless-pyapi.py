import json
import qless
import re
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound


class QlessPyapi(object):
    def __init__(self, qless_client):
        self.client = qless_client
        self.config = {
            'groups': {
                # 'all': '.*',
                'ungrouped': '$',
                'simon': {
                    'foobar': {
                        'foo': 'foo-.*',
                        'bar': 'bar-.*'
                    },
                    'example': 'sample-.*'
                },
                'thorsten': {
                    'example': {
                        'sample': 'sample-.*',
                        'foobar': {
                            'foobar': 'foobar-.*',
                            'foo': 'foo-.*',
                            'bar': 'bar-.*'
                        },
                    },
                    'foobar': '(foo)?bar-.*'
                },
                'sample': 'sample-.*'
            }
            # 'groups': {
            #     'sample': 'sample-.*',
            #     'foobar': 'foobar-.*',
            #     'foo': 'foo-.*',
            #     'bar': 'bar-.*',
            # },
        }
        self.url_map = Map([
            Rule('/config', endpoint='config'),
            Rule('/groups', endpoint='groups'),
            Rule('/groups/<regex_str>', endpoint='groups_get'),
            Rule('/groups/$', endpoint='groups_get_ungrouped'),
            Rule('/queues', endpoint='queues'),
            Rule('/queues/<queue_name>', endpoint='queues_get'),
            Rule('/queues/<queue_name>/pause', endpoint='queues_pause'),
            Rule('/queues/<queue_name>/unpause', endpoint='queues_unpause'),
            Rule('/queues/<queue_name>/stats', endpoint='queues_stats'),
        ])

    def on_config(self, request):
        return Response(json.dumps(self.config), content_type='application/json')

    def on_groups(self, request):
        groups = self.group_to_navtree('Groups', self.config['groups'])
        return Response(json.dumps(groups['children']), content_type='application/json')

    def group_to_navtree(self, name, data):
        if isinstance(data, basestring):
            return {
                'label': name,
                'data': data
            }
        else:
            return {
                'label': name,
                'children': [self.group_to_navtree(group_name, group_data) for (group_name, group_data) in data.items()]
            }

    def on_groups_get(self, request, regex_str):
        regex = re.compile("(?:" + regex_str + r")\Z")
        queues = [queue for queue in self.client.queues.counts if regex.match(queue['name'])]
        return Response(json.dumps(queues), content_type='application/json')

    def on_groups_get_ungrouped(self, request):
        queues = self.queues_remove_group_matches(self.client.queues.counts, self.config['groups'])
        return Response(json.dumps(queues), content_type='application/json')

    def queues_remove_group_matches(self, queues, data):
        if isinstance(data, basestring):
            regex = re.compile("(?:" + data + r")\Z")
            return [queue for queue in queues if not regex.match(queue['name'])]
        else:
            for group_data in data.values():
                queues = self.queues_remove_group_matches(queues, group_data)
            return queues

    def on_queues(self, request):
        queues = self.client.queues.counts
        return Response(json.dumps(queues), content_type='application/json')

    def on_queues_get(self, request, queue_name):
        queue = self.client.queues[queue_name].counts
        return Response(json.dumps(queue), content_type='application/json')

    def on_queues_pause(self, request, queue_name):
        ret = self.client.queues[queue_name].pause()
        return Response(json.dumps(ret), content_type='application/json')

    def on_queues_unpause(self, request, queue_name):
        ret = self.client.queues[queue_name].unpause()
        return Response(json.dumps(ret), content_type='application/json')

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
