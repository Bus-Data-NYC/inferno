# Copyright 2018, 2019 TransitCenter http://transitcenter.org

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM debian:stable

ENV CALLS=calls \
    POSITIONS=rt_vehicle_positions \
    EPSG=3627 \
    INFERNOFLAGS="--quiet --incomplete"

RUN apt-get -y update; apt-get -y install gnupg2 wget ca-certificates rpl pwgen
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get -y update; \
    apt-get install -y --no-install-recommends -y \
    postgresql-client-10 \
    python3-pip \
    python3-numpy \
    python3-psycopg2 \
    && pip3 install 'pytz>=2015.6' \
    && apt-get clean

RUN echo "kernel.shmmax=543252480" >> /etc/sysctl.conf
RUN echo "kernel.shmall=2097152" >> /etc/sysctl.conf

COPY src/inferno.py inferno.py

ENTRYPOINT ./inferno.py ${INFERNOFLAGS} --epsg=${EPSG} --calls=${CALLS} --positions=${POSITIONS} ${DATE}
