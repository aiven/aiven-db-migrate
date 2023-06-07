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

# Install aiven-extras.
git clone https://github.com/aiven/aiven-extras /aiven-extras
git -C /aiven-extras checkout bf259febf0fd1e76479d4d4f252cc36a4ff9767f
make -C /aiven-extras clean \
  aiven_extras.control \
  sql/aiven_extras--1.1.8.sql \
  sql/aiven_extras--1.1.7--1.1.8.sql

# Clone aiven-pg-security (aiven_gatekeeper): https://github.com/aiven/aiven-pg-security/releases/tag/v1.0.4
git clone https://github.com/aiven/aiven-pg-security/ /aiven-pg-security
git -C /aiven-pg-security checkout 3107f450ee765515a8a19c78e4f3125ce6ab1f36

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

    make -C /aiven-pg-security PG_CONFIG="/usr/lib/postgresql/${dest}/bin/pg_config" clean install

    # Count we have 2 entries: aiven_gatekeeper.so and aiven_extras.so.
    $(ls /usr/lib/postgresql/${dest}/lib/aiven_{gatekeeper,extras}.so | wc -l | grep -q 2) || exit 1
done
