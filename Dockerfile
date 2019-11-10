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

ENV CALLS calls
ENV POSITIONS rt_vehicle_positions
ENV EPSG 3627

RUN apt-get -y update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        gnupg2 \
        postgresql-client \
        python3-pip \
        python3-setuptools \
    && apt-get clean

RUN echo "kernel.shmmax=543252480" >> /etc/sysctl.conf
RUN echo "kernel.shmall=2097152" >> /etc/sysctl.conf

COPY requirements.txt requirements.txt
RUN python3 -m pip install -r requirements.txt

COPY src/inferno.py inferno.py

ENTRYPOINT ["./inferno.py", "--quiet", "--incomplete"]
CMD ["2019-10-01"]
