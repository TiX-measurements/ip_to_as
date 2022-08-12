FROM python:3.8.5

RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y python3-dev

WORKDIR /app
COPY ./src /app/

RUN pip install -r requirements.txt

ENV DB_HOST=mysql
ENV DB_USER=tix
ENV DB_PASS=tix
ENV DB_ROOT_USER=root
ENV DB_ROOT_USER_PASS=tix

#CMD ["shell"]

CMD ["sh", "/app/exe.sh"]
