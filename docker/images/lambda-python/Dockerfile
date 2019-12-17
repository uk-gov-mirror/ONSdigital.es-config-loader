FROM python:3.7-stretch
ARG reqs=dev-
ARG huid=9999

# Create user called "docker" with selected UID
RUN useradd --shell /bin/bash -u $huid -o -c "" -m dockeruser

# create pre-commit cache dir
RUN mkdir -p /home/dockeruser/.cache/pre-commit && \
chown -R dockeruser:dockeruser /home/dockeruser/.cache

RUN curl https://dl.google.com/go/go1.12.6.linux-amd64.tar.gz | \
tar -C /opt -xz && ln -s /opt/go/bin/go /usr/local/bin/go

COPY ${reqs}requirements.txt .
RUN pip install --upgrade pip pipenv && \
pip install -r ${reqs}requirements.txt

USER dockeruser
WORKDIR /usr/src/app
