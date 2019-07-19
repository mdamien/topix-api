from flask import Flask, escape, request, jsonify
from flask_cors import cross_origin
from celery import Celery
import subprocess
import time, uuid, os

EXTRACT_CMD = "dtrx -n ./jobs/{job_id}.zip --one=rename && mv {job_id} jobs/"
RUN_PRE_PROCESS_CMD = """docker exec -it 804c7eb326e9 python3 /data/mcorneli/WDTopix/build_data.py /data/dmarie/topix-api/jobs/{job_id}/"""
RUN_TOPIX_CMD = """docker exec -it 804c7eb326e9 /data/dmarie/topix/link/build/Topix 3 3 2 2 3 3 25 0 1 100 0.0001 5 5 /data/dmarie/topix-api/jobs/{job_id}/"""


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


def call(cmd, live=False, log_file=None):
    print("exec", cmd)
    if not live:
        r = subprocess.run(cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=type(cmd) is str, universal_newlines=True)  
        print("returned >", r.stdout)
        print("returned err >", r.stderr)
    else:
        log = open(log_file, 'w')
        for line in os.popen(cmd):
            print(' > ' + line)
            log.write(line)
            log.flush()


@celery.task()
def run_topix(url, job_id):
    dir = 'jobs/' + job_id
    call(['wget', url, '-O', dir + ".zip"])
    open(dir + '.download_done', 'w').write('')
    call(EXTRACT_CMD.format(job_id=job_id))
    open(dir + '.extract_done', 'w').write('')
    call(RUN_PRE_PROCESS_CMD.format(job_id=job_id), live=True, log_file=dir + '.pre_process.log')
    open(job_id + '.pre_process_done', 'w').write('')
    call('mkdir ' + dir + '/out')
    call(RUN_TOPIX_CMD.format(job_id=job_id), live=True, log_file=dir + '.topix.log')
    open(job_id + '.topix_done', 'w').write('')


@app.route('/')
def hello():
    return """
    <pre>Hello, this is the topix-api server
    <form method="GET" action="/process/"><input name="url" value="http://server/sample.zip"/><input type=submit>
    """


@app.route('/process/')
@cross_origin()
def process():
    url = request.args.get("url")
    job_id = str(uuid.uuid4())
    run_topix.delay(url, job_id)
    return jsonify({
        'job_id': job_id,
        'result_url': ('http://81a47648.ngrok.io/result/?job_id=' + job_id),
    })


@app.route('/result/')
@cross_origin()
def result():
    job_id = request.args.get("job_id")

    pre_process_log = None
    try:
        pre_process_log = open(
            "jobs/" + job_id + ".pre_process.log"
        ).readlines()
    except:
        pass

    topix_log = None
    try:
        topix_log = open(
            "jobs/" + job_id + ".topix.log"
        ).readlines()
    except:
        pass

    return jsonify({
        'log': "processing %s" % job_id,
        'pre_process_log': pre_process_log,
        'topix_log': topix_log,
        'download_done':
            os.path.exists("jobs/" + job_id + ".download_done"),
        'extract_done':
            os.path.exists("jobs/" + job_id + ".extract_done"),
        'pre_process_done':
            os.path.exists("jobs/" + job_id + ".pre_process_done"),
        'topix_done':
            os.path.exists("jobs/" + job_id + ".topix_done"),
    })
