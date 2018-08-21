FROM python:3.7.0-alpine

RUN pip install requests
RUN pip install redis

COPY infraexport.py /infraexport.py

CMD ["python", "/infraexport.py"]

