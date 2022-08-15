FROM python:3.8.5

RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y python3-dev

WORKDIR /app

# copy the requirements file first so we don't have to reinstall everything
# after a python file is changed
COPY ./src/requirements.txt /app/
RUN pip install -r requirements.txt

COPY ./src /app/

ENV DB_HOST=mysql
ENV DB_USER=tix
ENV DB_PASS=tix
ENV DB_ROOT_USER=root
ENV DB_ROOT_USER_PASS=tix

CMD ["sh", "/app/exe.sh"]
