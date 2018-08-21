FROM python:3.7.0-alpine

RUN pip install requests
RUN pip install redis

COPY metrics.conf /metrics.conf
COPY metricexport.py /metricexport.py

CMD ["python", "/metricexport.py"]

