%global project aiven-db-migrate

Name:           %{project}
Version:        %{major_version}
Release:        %{release_number}%{?dist}
Url:            https://github.com/aiven/%{project}
Source0:        %{source_dist}
Summary:        Aiven database migration tool
License:        ASL 2.0
BuildArch:      noarch
BuildRequires:  python3-devel
BuildRequires:  python3dist(wheel)
BuildRequires:  python3dist(hatch-vcs)
BuildRequires:  python3dist(packaging)
BuildRequires:  python3dist(psycopg2)

%description
Aiven is a next-generation managed cloud services.  Its focus is in ease of
adoption, high fault resilience, customer's peace of mind and advanced
features at competitive price points.

Aiven database migration tool. This tool is meant for easy migration of databases from some database service
provider, such AWS RDS, or on premises data center, to [Aiven Database as a Service](https://aiven.io/).
However, it's not limited for Aiven services and it might be useful as a generic database migration tool.

%prep
%autosetup -n aiven_db_migrate-%{version}

%generate_buildrequires
%pyproject_buildrequires

%build
%pyproject_wheel

%install
%pyproject_install
%pyproject_save_files aiven_db_migrate

%check
%pyproject_check_import

%files -n %{name} -f %{pyproject_files}
%license LICENSE
%doc README.md
%{_bindir}/pg_migrate

%changelog
* Wed Dec 08 2024 Aiven Support <support@aiven.io>
- Initial
