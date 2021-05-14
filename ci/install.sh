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
apt-get install -y postgresql-{9.5,9.6,10,11,12,13} postgresql-{9.5,9.6,10,11,12,13}-postgis-3 postgresql-{9.5,9.6,10,11,12,13}-pgextwlist
git clone https://github.com/aiven/aiven-extras /aiven-extras
make -C /aiven-extras clean rpm
# maybe add a deb target to aiven-extras in the future, but for now, while hacky, this is (probably) terser and less intrusive

for dest in "9.6" "10" "11" "12" "13"
do
    cp /aiven-extras/build/aiven_extras.control /usr/share/postgresql/${dest}/extension/
    cp /aiven-extras/sql/*.sql /usr/share/postgresql/${dest}/extension/
    cp /aiven-extras/build/sql/*.sql /usr/share/postgresql/${dest}/extension/
done

