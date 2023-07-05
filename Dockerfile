FROM python:3.11.3-slim-bullseye

ARG VERSION=0.1.5

RUN useradd -ms /bin/bash dans

USER dans
WORKDIR /home/dans
ENV PYTHONPATH=/home/dans/packaging-service/src
ENV BASE_DIR=/home/dans/packaging-service
RUN mkdir -p ${BASE_DIR}

COPY ./dist/*.* .

#
RUN mkdir -p ${BASE_DIR} && \
    pip install --no-cache-dir *.whl && rm -rf *.whl && \
    tar xf packaging_service-${VERSION}.tar.gz -C ${BASE_DIR} --strip-components 1

WORKDIR ${BASE_DIR}

CMD ["python", "src/main.py"]
#CMD ["tail", "-f", "/dev/null"]