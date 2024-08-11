FROM python:3.8-slim

ENV APP_HOME="/home/superside"
ENV PYTHONFAULTHANDLER=1
RUN useradd --create-home superside
WORKDIR ${APP_HOME}

COPY . ${APP_HOME}
RUN pip install -r requirements.txt

RUN chown -R superside:superside .
USER superside

ENTRYPOINT ["python3","src/superside_etl.py"]
