from json import JSONEncoder
from qless import Job


class QlessJSONEncoder(JSONEncoder):
    def default(self, o):
        try:
            if type(o) is Job:
                return {
                    'data': o.data,
                    'dependencies': o.dependencies,
                    'dependents': o.dependents,
                    'expires_at': o.expires_at,
                    'failure': o.failure,
                    'history': o.history,
                    'jid': o.jid,
                    'klass_name': o.klass_name,
                    'original_retries': o.original_retries,
                    'priority': o.priority,
                    'queue_name': o.queue_name,
                    'retries_left': o.retries_left,
                    'state': o.state,
                    'tags': o.tags,
                    'tracked': o.tracked,
                    'worker_name': o.worker_name,
                }

        except TypeError:
            pass

        return JSONEncoder.default(self, o)
