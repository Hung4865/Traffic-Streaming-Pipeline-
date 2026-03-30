FROM flink:1.17.1

RUN apt-get update -y && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    python3 \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /opt/flink

RUN pip install apache-flink==1.17.1
RUN pip install apache-flink-libraries==1.17.1
RUN pip install psycopg2-binary==2.9.9

COPY ./connectors/flink-connector-kafka-1.17.1.jar ./lib/
COPY ./connectors/kafka-clients-3.5.1.jar ./lib/
COPY flink_processor.py /opt/flink/flink_processor.py

CMD ['bash', 'run.sh']