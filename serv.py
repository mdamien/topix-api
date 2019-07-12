from flask import Flask, escape, request
from celery import Celery
import time


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
def add_together(a, b):
    time.sleep(10)
    print(a, b, ':>', a + b)


@app.route('/')
def hello():
    return """
    <pre>Hello, this is the topix-api server
    <form method="GET" action="/process/"><input name="url" value="http://server/sample.zip"/><input type=submit>
    """


@app.route('/process/')
def process():
    url = request.args.get("url")
    add_together.delay(42, 42)
    return '<pre>let\'s process ' + escape(url) + ', result in /result?job_id=123!'


@app.route('/result/')
def result():
    job_id = request.args.get("job_id")
    return '<pre>result of ' + escape(job_id) + ': ...'
