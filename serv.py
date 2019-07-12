from flask import Flask, escape, request

app = Flask(__name__)

@app.route('/')
def hello():
    return """
    <pre>Hello, this is the topix-api server
    <form method="GET" action="/process/"><input name="url" value="http://server/sample.zip"/><input type=submit>
    """


@app.route('/process/')
def process():
    url = request.args.get("url")
    return f'<pre>let\'s process {escape(url)}, result in /result?job_id=123!'


@app.route('/result/')
def result():
    job_id = request.args.get("job_id")
    return f'<pre>result of {escape(job_id)}: ...'
