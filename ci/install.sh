#!/usr/bin/env bash
set -e

AIVEN_EXTRAS_TARGET="/aiven-extras"
AIVEN_PG_SECURITY_TARGET="/aiven-pg-security"

export DEBIAN_FRONTEND=noninteractive
apt-get -y update
ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
apt install -y lsb-release wget gnupg tzdata git make rpm python3-pip libpq-dev jq
dpkg-reconfigure --frontend noninteractive tzdata
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get -y update
apt-get install -y \
  postgresql-{13,14,15,16,17} \
  postgresql-server-dev-{13,14,15,16,17} \
  postgresql-{13,14,15,16,17}-postgis-3 \
  postgresql-{13,14,15,16,17}-pgextwlist

# Install aiven-extras, using the latest tag.
git clone https://github.com/aiven/aiven-extras "${AIVEN_EXTRAS_TARGET}"
git -C "${AIVEN_EXTRAS_TARGET}" checkout "$(git -C "${AIVEN_EXTRAS_TARGET}" describe --tags --abbrev=0)"
short_version=$(grep -oP 'short_ver = \K\d+\.\d+\.\d+' "${AIVEN_EXTRAS_TARGET}/Makefile")
last_version=$(grep -oP 'last_ver = \K\d+\.\d+\.\d+' "${AIVEN_EXTRAS_TARGET}/Makefile")
make -C "${AIVEN_EXTRAS_TARGET}" clean \
  aiven_extras.control \
  "sql/aiven_extras--${short_version}.sql" \
  "sql/aiven_extras--${last_version}--${short_version}.sql"

# The latest released version of aiven-pg-security (excludes pre-releases).
AIVEN_PG_SECURITY_TAG=$(wget --quiet -O - "https://api.github.com/repos/aiven/aiven-pg-security/releases/latest" | jq -r .tag_name)
# Clone aiven-pg-security (aiven_gatekeeper).
git clone https://github.com/aiven/aiven-pg-security/ "${AIVEN_PG_SECURITY_TARGET}"
git -C "${AIVEN_PG_SECURITY_TARGET}" checkout ${AIVEN_PG_SECURITY_TAG}

# maybe add a deb target to aiven-extras in the future, but for now, while hacky, this is (probably) terser and less intrusive

for dest in "13" "14" "15" "16" "17"
do
    gcc -fPIC -I/usr/include/postgresql/${dest}/server \
      -D_GNU_SOURCE -I/usr/include/libxml2  -I/usr/include -c -o aiven_extras.o $AIVEN_EXTRAS_TARGET/src/aiven_extras.c
    gcc -fPIC -shared -o aiven_extras.so aiven_extras.o -L/usr/lib/postgresql/${dest} \
      -L/usr/lib64  -L/usr/lib64 -Wl,--as-needed -Wl,-rpath,/usr/lib/postgresql/${dest},--enable-new-dtags

    mkdir -p /usr/lib/postgresql/${dest}/lib/
    cp aiven_extras.so /usr/lib/postgresql/${dest}/lib/
    cp $AIVEN_EXTRAS_TARGET/aiven_extras.control /usr/share/postgresql/${dest}/extension/
    cp $AIVEN_EXTRAS_TARGET/sql/*.sql /usr/share/postgresql/${dest}/extension/

    make -C "$AIVEN_PG_SECURITY_TARGET" PG_CONFIG="/usr/lib/postgresql/${dest}/bin/pg_config" clean install

    # Count we have 2 entries: aiven_gatekeeper.so and aiven_extras.so.
    $(ls /usr/lib/postgresql/${dest}/lib/aiven_{gatekeeper,extras}.so | wc -l | grep -q 2) || exit 1
done
