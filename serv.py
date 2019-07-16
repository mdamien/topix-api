from flask import Flask, escape, request
from celery import Celery
import time, uuid

DOWNLOAD_CMD = """sudo docker exec -it 804c7eb326e9 python3 -i /data/mcorneli/WDTopix/build_data.py /data/dmarie/topix-api/jobs/{job_id}"""
RUN_PRE_PROCESS_CMD = """sudo docker exec -it 804c7eb326e9 python3 -i /data/mcorneli/WDTopix/build_data.py /data/dmarie/topix-api/jobs/{job_id}"""
RUN_TOPIX_CMD = """sudo docker exec -it 804c7eb326e9 /data/dmarie/topix/link/build/Topix 3 3 2 2 3 3 25 0 1 100 0.0001 5 5 /data/mcorneli/WDTopix/build_data.py /data/dmarie/topix-api/jobs/{job_id}"""


def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery


app = Flask(__name__)
app.config.update(
    CELERY_BROKER_URL='redis://localhost:6379',
    CELERY_RESULT_BACKEND='redis://localhost:6379'
)
celery = make_celery(app)


@celery.task()
def run_topix(url):
    job_id = str(uuid.uuid4())
    print('download ' + url + ' to ./jobs/' + job_id + ' ...')
    print(DOWNLOAD_CMD.format(url=url))
    print(RUN_PRE_PROCESS_CMD.format(job_id=job_id))
    print(RUN_TOPIX_CMD.format(job_id=job_id))


@app.route('/')
def hello():
    return """
    <pre>Hello, this is the topix-api server
    <form method="GET" action="/process/"><input name="url" value="http://server/sample.zip"/><input type=submit>
    """


@app.route('/process/')
def process():
    url = request.args.get("url")
    run_topix.delay(url)
    return '<pre>let\'s process ' + escape(url) + ', result in /result?job_id=123!'


@app.route('/result/')
def result():
    job_id = request.args.get("job_id")
    return '<pre>result of ' + escape(job_id) + ': ...'
