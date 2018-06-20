FROM debian:stable

ENV CALLSTABLE=calls \
    POSITIONSTABLE=rt_vehicle_positions

RUN apt -y update; apt -y install gnupg2 wget ca-certificates rpl pwgen
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get update; apt-get install -y postgresql-client-10

WORKDIR /

RUN apt-get install --no-install-recommends -y python3-pip python3-numpy python3-psycopg2 && \
    pip3 install 'pytz>=2015.6'

RUN echo "kernel.shmmax=543252480" >> /etc/sysctl.conf
RUN echo "kernel.shmall=2097152" >> /etc/sysctl.conf

# copy inferno
COPY src/inferno.py ./src/inferno.py

ENTRYPOINT python3 ./src/inferno.py ${DATE} --calls-table ${CALLSTABLE} --positions-table ${POSITIONSTABLE}
