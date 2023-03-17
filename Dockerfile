FROM bitnami/spark:latest

COPY ./data-analysis.py /opt/spark/work-dir/data-analysis.py
COPY requirements.txt /opt/spark/work-dir/requirements.txt

RUN pip install -r /opt/spark/work-dir/requirements.txt
RUN python data-analysis.py