from __future__ import print_function

import json
import qless
import re
import sys

from QlessJSONEncoder import QlessJSONEncoder
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound


class QlessPyapi(object):
    def __init__(self):
        # default config
        self.config = {
            'redis': 'redis://localhost',
            'groups': {
                'ungrouped': '$'
            }
        }

        # load config
        try:
            with open('config.json') as cfg:
                self.config = json.load(cfg)
        except Exception as e:
            print("Failed to load config file: " + str(e), file=sys.stderr)
            pass

        self.client = qless.Client(self.config['redis'])

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
            Rule('/queues/<queue_name>/<any(waiting, running, stalled, scheduled, depends, recurring):state>/' +
                 '<int:start>/<int:limit>', endpoint='queues_jobs'),
            Rule('/workers', endpoint='workers'),
            Rule('/workers/<worker_name>', endpoint='workers_get'),
            Rule('/jobs/<string(length=32):jid>', endpoint='jobs_get'),
            Rule('/jobs/<string(length=32):jid>/cancel', endpoint='jobs_cancel'),
            Rule('/jobs/<string(length=32):jid>/retry', endpoint='jobs_retry'),
            Rule('/jobs/<string(length=32):jid>/priority', endpoint='jobs_priority'),
            Rule('/jobs/<string(length=32):jid>/tag', endpoint='jobs_tag'),
            Rule('/jobs/<string(length=32):jid>/untag', endpoint='jobs_untag'),
            Rule('/jobs/<string(length=32):jid>/track', endpoint='jobs_track'),
            Rule('/jobs/<string(length=32):jid>/untrack', endpoint='jobs_untrack'),
            Rule('/jobs/tracked', endpoint='jobs_tracked'),
            Rule('/jobs/failed', endpoint='jobs_failed'),
            Rule('/jobs/failed/<group>/<int:start>/<int:limit>', endpoint='jobs_failed_list'),
            Rule('/jobs/failed/<group>/cancel', endpoint='jobs_failed_list_cancel'),
            Rule('/jobs/failed/<group>/retry', endpoint='jobs_failed_list_retry'),
            Rule('/jobs/completed/<int:start>/<int:limit>', endpoint='jobs_completed'),
        ])

    def json_response(self, content):
        return Response(json.dumps(content, default=QlessJSONEncoder().default), content_type='application/json')

    def on_config(self, request):
        return self.json_response(self.config)

    def on_groups(self, request):
        groups = self.group_to_navtree('Groups', self.config['groups'])
        return self.json_response(groups['children'])

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
        return self.json_response(queues)

    def on_groups_get_ungrouped(self, request):
        queues = self.queues_remove_group_matches(self.client.queues.counts, self.config['groups'])
        return self.json_response(queues)

    def queues_remove_group_matches(self, queues, data):
        if isinstance(data, basestring):
            regex = re.compile("(?:" + data + r")\Z")
            return [queue for queue in queues if not regex.match(queue['name'])]
        else:
            for group_data in data.values():
                queues = self.queues_remove_group_matches(queues, group_data)
            return queues

    def on_queues(self, request):
        return self.json_response(self.client.queues.counts)

    def on_queues_get(self, request, queue_name):
        return self.json_response(self.client.queues[queue_name].counts)

    def on_queues_pause(self, request, queue_name):
        return self.json_response(self.client.queues[queue_name].pause())

    def on_queues_unpause(self, request, queue_name):
        return self.json_response(self.client.queues[queue_name].unpause())

    def on_queues_stats(self, request, queue_name):
        return self.json_response(self.client.queues[queue_name].stats())

    def on_queues_jobs(self, request, state, queue_name, start, limit):
        total = self.client.queues[queue_name].counts[state]

        if state == 'waiting':
            jobs = self.client.queues[queue_name].peek(limit)[start:limit]
        else:
            jids = getattr(self.client.queues[queue_name].jobs, state)(start, limit)
            jobs = self.client.jobs.get(*jids)

        return self.json_response({'total': total, 'jobs': jobs})

    def on_workers(self, request):
        return self.json_response(self.client.workers.counts)

    def on_workers_get(self, request, worker_name):
        worker = self.client.workers[worker_name]
        worker['jobs'] = self.client.jobs.get(*worker['jobs'])
        worker['stalled'] = self.client.jobs.get(*worker['stalled'])
        return self.json_response(worker)

    def get_job(self, jid):
        job = self.client.jobs.get(jid)
        if len(job) <= 0:
            raise NotFound()
        return job[0]

    def on_jobs_get(self, request, jid):
        return self.json_response(self.get_job(jid))

    def on_jobs_cancel(self, request, jid):
        return self.json_response(self.get_job(jid).cancel())

    def on_jobs_retry(self, request, jid):
        job = self.get_job(jid)
        return self.json_response(job.move(job.queue_name))

    def on_jobs_priority(self, request, jid):
        job = self.get_job(jid)
        job.priority = request.data
        return self.json_response(job)

    def on_jobs_tag(self, request, jid):
        job = self.get_job(jid)
        return self.json_response(json.loads(json.loads(job.tag(request.data))))

    def on_jobs_untag(self, request, jid):
        job = self.get_job(jid)
        return self.json_response(json.loads(job.untag(request.data)))

    def on_jobs_track(self, request, jid):
        job = self.get_job(jid)
        return self.json_response(job.track())

    def on_jobs_untrack(self, request, jid):
        job = self.get_job(jid)
        return self.json_response(job.untrack())

    def on_jobs_tracked(self, request):
        return self.json_response(self.client.jobs.tracked())

    def on_jobs_failed(self, request):
        return self.json_response(self.client.jobs.failed())

    def on_jobs_failed_list(self, request, group, start, limit):
        return self.json_response(self.client.jobs.failed(group, start, limit))

    def on_jobs_failed_list_cancel(self, request, group):
        failed = self.client.jobs.failed(group, 0, 1000)
        res = []
        for job in failed['jobs']:
            res.append(job.cancel())

        return self.json_response(res)

    def on_jobs_failed_list_retry(self, request, group):
        failed = self.client.jobs.failed(group, 0, 1000)
        res = []
        for job in failed['jobs']:
            res.append(job.move(job.queue_name))

        return self.json_response(res)

    def on_jobs_completed(self, request, start, limit):
        jids = self.client.jobs.complete(start, limit)
        jobs = self.client.jobs.get(*jids)
        total = len(self.client.jobs.complete(0, 1000))  # TODO: FIX ME
        return self.json_response({'total': total, 'jobs': jobs})

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except qless.QlessException, e:
            return Response(e.message, status=500)
        except HTTPException, e:
            return e

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)


def create_app():
    return QlessPyapi()


if __name__ == '__main__':
    from werkzeug.serving import run_simple

    app = create_app()
    run_simple('0.0.0.0', 4000, app)
    # reloader_type='watchdog'
