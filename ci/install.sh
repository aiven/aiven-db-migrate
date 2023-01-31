#!/usr/bin/env bash
set -e

export DEBIAN_FRONTEND=noninteractive
apt-get -y update
ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
apt install -y lsb-release wget gnupg tzdata git make rpm python3-pip libpq-dev
dpkg-reconfigure --frontend noninteractive tzdata
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get -y update
apt-get install -y \
  postgresql-{10,11,12,13,14} \
  postgresql-server-dev-{10,11,12,13,14} \
  postgresql-{10,11,12,13,14}-postgis-3 \
  postgresql-{10,11,12,13,14}-pgextwlist
git clone https://github.com/aiven/aiven-extras /aiven-extras
git -C /aiven-extras checkout bf259febf0fd1e76479d4d4f252cc36a4ff9767f
make -C /aiven-extras clean \
  aiven_extras.control \
  sql/aiven_extras--1.1.8.sql \
  sql/aiven_extras--1.1.7--1.1.8.sql
# maybe add a deb target to aiven-extras in the future, but for now, while hacky, this is (probably) terser and less intrusive

for dest in "10" "11" "12" "13" "14"
do
    gcc -fPIC -I/usr/include/postgresql/${dest}/server \
      -D_GNU_SOURCE -I/usr/include/libxml2  -I/usr/include -c -o standby_slots.o /aiven-extras/src/standby_slots.c
    gcc -fPIC -shared -o aiven_extras.so standby_slots.o -L/usr/lib/postgresql/${dest} \
      -L/usr/lib64  -L/usr/lib64 -Wl,--as-needed -Wl,-rpath,/usr/lib/postgresql/${dest},--enable-new-dtags
    mkdir -p /usr/lib/postgresql/${dest}/lib/
    cp aiven_extras.so /usr/lib/postgresql/${dest}/lib/
    cp /aiven-extras/aiven_extras.control /usr/share/postgresql/${dest}/extension/
    cp /aiven-extras/sql/*.sql /usr/share/postgresql/${dest}/extension/
done

