#!/usr/bin/env python2
from __future__ import print_function

import json
import re
import sys

from os import path
from qless import Client, QlessException
from QlessJSONEncoder import QlessJSONEncoder
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.routing import Map, Rule
from werkzeug.utils import redirect
from werkzeug.wrappers import Request, Response
from werkzeug.wsgi import SharedDataMiddleware, DispatcherMiddleware

QLESS_MAX_PEEK = 1000


def json_response(content):
    return Response(json.dumps(content, default=QlessJSONEncoder().default), content_type='application/json')


class Config:
    def __init__(self, config_file='config.json'):
        # default config
        self.default_config = {
            'hostname': '127.0.0.1',
            'port': 4000,
            'ui': True,
            'redis': 'redis://localhost',
            'groups': {
                'ungrouped': '$'
            }
        }

        # empty config
        self.config = {}

        # load config
        try:
            with open(config_file) as cfg:
                self.config = json.load(cfg)
        except Exception as e:
            print('Failed to load config file:', str(e), file=sys.stderr)
            print('[config]', 'using default config:', self.default_config)
            pass

    def __getitem__(self, item):
        if item in self.config:
            return self.config[item]
        else:
            default = self.default_config[item]
            print('[config]', 'using default for `' + item + '`:', default)
            return default


class QlessPyapi(object):
    def __init__(self, config):
        self.config = config
        self.client = Client(self.config['redis'])

        self.url_map = Map([
            Rule('/groups', endpoint='groups'),
            Rule('/groups/nav_tree', endpoint='groups_nav_tree'),
            Rule('/groups/queues/<string:regex_str>', endpoint='groups_get_queues'),
            Rule('/groups/queues/$', endpoint='groups_get_queues_ungrouped'),
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
            Rule('/jobs/<string(length=32):jid>/cancel_subtree', endpoint='jobs_cancel_subtree'),
            Rule('/jobs/<string(length=32):jid>/retry', endpoint='jobs_retry'),
            Rule('/jobs/<string(length=32):jid>/priority', endpoint='jobs_priority'),
            Rule('/jobs/<string(length=32):jid>/move', endpoint='jobs_move_queue'),
            Rule('/jobs/<string(length=32):jid>/tag', endpoint='jobs_tag'),
            Rule('/jobs/<string(length=32):jid>/untag', endpoint='jobs_untag'),
            Rule('/jobs/<string(length=32):jid>/track', endpoint='jobs_track'),
            Rule('/jobs/<string(length=32):jid>/untrack', endpoint='jobs_untrack'),
            Rule('/jobs/<string(length=32):jid>/depend', endpoint='jobs_depend'),
            Rule('/jobs/<string(length=32):jid>/undepend', endpoint='jobs_undepend'),
            Rule('/jobs/<string(length=32):jid>/trees', endpoint='jobs_dependency_trees'),
            Rule('/jobs/cancel', endpoint='jobs_cancel_list'),
            Rule('/jobs/tracked', endpoint='jobs_tracked'),
            Rule('/jobs/failed', endpoint='jobs_failed'),
            Rule('/jobs/failed/<group>/<int:start>/<int:limit>', endpoint='jobs_failed_list'),
            Rule('/jobs/failed/<group>/cancel', endpoint='jobs_failed_list_cancel'),
            Rule('/jobs/failed/<group>/retry', endpoint='jobs_failed_list_retry'),
            Rule('/jobs/completed/<int:start>/<int:limit>', endpoint='jobs_completed'),
            Rule('/tags', endpoint='tags'),
            Rule('/tags/<tag>/<int:start>/<int:limit>', endpoint='tags_get'),
        ])

    def on_groups(self, request):
        return json_response(self.config['groups'])

    def on_groups_nav_tree(self, request):
        groups = self.group_to_nav_tree('Groups', self.config['groups'])
        return json_response(groups['children'])

    def group_to_nav_tree(self, name, data):
        if not isinstance(data, dict):
            return {
                'label': name,
                'data': data
            }
        else:
            return {
                'label': name,
                'children': [self.group_to_nav_tree(group_name, group_data) for (group_name, group_data) in
                             data.items()]
            }

    def on_groups_get_queues(self, request, regex_str):
        regex = re.compile("(?:" + regex_str + r")\Z")
        queues = [queue for queue in self.client.queues.counts if regex.match(queue['name'])]
        return json_response(queues)

    def on_groups_get_queues_ungrouped(self, request):
        queues = self.queues_remove_group_matches(self.client.queues.counts, self.config['groups'])
        return json_response(queues)

    def queues_remove_group_matches(self, queues, data):
        if not isinstance(data, dict):
            regex = re.compile("(?:" + data + r")\Z")
            return [queue for queue in queues if not regex.match(queue['name'])]
        else:
            for group_data in data.values():
                queues = self.queues_remove_group_matches(queues, group_data)
            return queues

    def on_queues(self, request):
        queues = self.client.queues.counts

        # qless-core returns {} instead of an empty array
        if isinstance(queues, dict):
            return json_response([])

        return json_response(queues)

    def on_queues_get(self, request, queue_name):
        return json_response(self.client.queues[queue_name].counts)

    def on_queues_pause(self, request, queue_name):
        return json_response(self.client.queues[queue_name].pause())

    def on_queues_unpause(self, request, queue_name):
        return json_response(self.client.queues[queue_name].unpause())

    def on_queues_stats(self, request, queue_name):
        return json_response(self.client.queues[queue_name].stats())

    def on_queues_jobs(self, request, state, queue_name, start, limit):
        total = self.client.queues[queue_name].counts[state]

        if state == 'waiting':
            jobs = self.client.queues[queue_name].peek(limit)[start:limit]
        else:
            jid_list = getattr(self.client.queues[queue_name].jobs, state)(start, limit)
            jobs = self.client.jobs.get(*jid_list)

        return json_response({'total': total, 'jobs': jobs})

    def on_workers(self, request):
        workers = self.client.workers.counts

        # qless-core returns {} instead of an empty array
        if isinstance(workers, dict):
            return json_response([])

        return json_response(workers)

    def on_workers_get(self, request, worker_name):
        worker = self.client.workers[worker_name]
        worker['jobs'] = self.client.jobs.get(*worker['jobs'])
        worker['stalled'] = self.client.jobs.get(*worker['stalled'])
        return json_response(worker)

    def get_job(self, jid):
        job = self.client.jobs.get(jid)
        if len(job) <= 0:
            raise NotFound()
        return job[0]

    def get_root_jobs(self, jid):
        root_jobs = []
        job = self.get_job(jid)

        if len(job.dependencies) == 0:
            return [job.jid]
        else:
            for dep in job.dependencies:
                for jid in self.get_root_jobs(dep):
                    root_jobs.append(jid)

        return set(root_jobs)

    def on_jobs_get(self, request, jid):
        job = self.get_job(jid)
        job.dependencies = self.client.jobs.get(*job.dependencies)
        job.dependents = self.client.jobs.get(*job.dependents)
        return json_response(job)

    def on_jobs_cancel(self, request, jid):
        return json_response(self.get_job(jid).cancel())

    def on_jobs_cancel_list(self, request):
        jobs = self.client.jobs.get(*json.loads(request.data))
        canceled = []

        for job in jobs:
            ret = job.cancel()

            for jid in ret:
                canceled.append(jid)

        return json_response(canceled)

    def on_jobs_cancel_subtree(self, request, jid):
        cancel_jid_list = []
        self.jobs_cancel_subtree(jid, cancel_jid_list)

        return json_response(cancel_jid_list)

    def jobs_cancel_subtree(self, jid, cancel_jid_list):
        job = self.get_job(jid)

        # do we have a child which leads to another leave?
        for dependent in job.dependents:
            if dependent not in cancel_jid_list:
                return

        # safe to do cancel ourselves
        cancel_jid_list.append(job.jid)

        # try to cancel our dependencies
        for dependency in job.dependencies:
            self.jobs_cancel_subtree(dependency, cancel_jid_list)

    def on_jobs_retry(self, request, jid):
        job = self.get_job(jid)
        return json_response(job.move(job.queue_name))

    def on_jobs_priority(self, request, jid):
        job = self.get_job(jid)
        job.priority = request.data
        return json_response(job)

    def on_jobs_move_queue(self, request, jid):
        job = self.get_job(jid)
        return json_response(job.move(request.data))

    def on_jobs_tag(self, request, jid):
        job = self.get_job(jid)
        return json_response(json.loads(json.loads(job.tag(request.data))))

    def on_jobs_untag(self, request, jid):
        job = self.get_job(jid)
        return json_response(json.loads(job.untag(request.data)))

    def on_jobs_track(self, request, jid):
        job = self.get_job(jid)
        return json_response(job.track())

    def on_jobs_untrack(self, request, jid):
        job = self.get_job(jid)
        return json_response(job.untrack())

    def on_jobs_depend(self, request, jid):
        job = self.get_job(jid)
        return json_response(job.depend(*json.loads(request.data)))

    def on_jobs_undepend(self, request, jid):
        job = self.get_job(jid)
        undepend = json.loads(request.data)

        if len(undepend) == 0:
            return json_response(job.undepend(all=True))
        else:
            return json_response(job.undepend(*undepend))

    def dependency_tree(self, jid):
        root_jobs = self.get_root_jobs(jid)
        trees = [self.dependency_subtree(root_jid, jid) for root_jid in root_jobs]

        from nltk.treeprettyprinter import TreePrettyPrinter
        from nltk import Tree
        return [TreePrettyPrinter(tree).text(unicodelines=True, html=True, maxwidth=32, nodedist=3)
                for tree in trees if isinstance(tree, Tree)]

    def dependency_subtree(self, jid, dest_jid):
        job = self.get_job(jid)
        label = jid + 'in ' + job.queue_name

        if len(job.dependents) == 0:
            return label
        else:
            from nltk import Tree
            return Tree(label, [self.dependency_subtree(jid, dest_jid) for jid in job.dependents])

    def on_jobs_dependency_trees(self, request, jid):
        return json_response(self.dependency_tree(jid))

    def on_jobs_tracked(self, request):
        return json_response(self.client.jobs.tracked())

    def on_jobs_failed(self, request):
        return json_response(self.client.jobs.failed())

    def on_jobs_failed_list(self, request, group, start, limit):
        return json_response(self.client.jobs.failed(group, start, limit))

    def on_jobs_failed_list_cancel(self, request, group):
        failed = self.client.jobs.failed(group, 0, 1000)
        res = []
        for job in failed['jobs']:
            res.append(job.cancel())

        return json_response(res)

    def on_jobs_failed_list_retry(self, request, group):
        failed = self.client.jobs.failed(group, 0, 1000)
        res = []
        for job in failed['jobs']:
            res.append(job.move(job.queue_name))

        return json_response(res)

    def on_jobs_completed(self, request, start, limit):
        jid_list = self.client.jobs.complete(start, limit)
        jobs = self.client.jobs.get(*jid_list)
        total = len(self.client.jobs.complete(0, QLESS_MAX_PEEK))  # TODO: FIX ME
        return json_response({'total': total, 'jobs': jobs})

    def on_tags(self, request):
        tags = self.client.tags(0, QLESS_MAX_PEEK)

        # qless-core returns {} instead of an empty array
        if isinstance(tags, dict):
            return json_response([])

        tags.sort()
        return json_response(tags)

    def on_tags_get(self, request, tag, start, limit):
        tag = self.client.jobs.tagged(tag, start, limit)
        tag['jobs'] = self.client.jobs.get(*tag['jobs'])
        return json_response(tag)

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except HTTPException as e:
            return e
        except QlessException as e:
            return Response(e.message, status=500)
        except Exception as e:
            return Response(e, status=500)

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)


def create():
    config = Config()
    app = QlessPyapi(config)

    if config['ui']:
        app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
            '/api': app.wsgi_app,
            '/app': SharedDataMiddleware(redirect('/app/index.html'), {
                '/': path.join(path.dirname(__file__), 'qless-ui', 'app')
            }),
            '/': redirect('/app/index.html')
        })

    return app, config


def run_server():
    from werkzeug.serving import run_simple

    (app, config) = create()
    run_simple(config['hostname'], config['port'], app, use_reloader=True)


if __name__ == '__main__':
    run_server()
