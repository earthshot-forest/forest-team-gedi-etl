FROM osgeo/gdal:ubuntu-small-3.2.0

WORKDIR /app

RUN apt-get update -y
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y
RUN apt-get install python3-psycopg2 -y
RUN apt-get install python3-gdal -y

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["python3", "./run_batch.py"]