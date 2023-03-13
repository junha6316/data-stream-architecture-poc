FROM bitnami/spark:latest

COPY app.py /opt/spark/work-dir/app.py
COPY requirements.txt /opt/spark/work-dir/requirements.txt

RUN pip install -r /opt/spark/work-dir/requirements.txt
RUN python app.py