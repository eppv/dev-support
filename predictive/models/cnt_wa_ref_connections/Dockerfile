
FROM python3.10-bullseye as builder
# Install Linux Packages
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \

RUN mkdir -p "/opt/model"

COPY . /opt/model/

RUN pip3 install --upgrade pip
RUN pip3 install --user --no-cache-dir -r requirements.txt

WORKDIR /opt/model/

FROM builder as main

ENV PYTHONPATH="/opt/model"

CMD []
