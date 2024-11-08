import os
from lxml import etree

# Define paths
sql_scripts_path = "/Users/avotech/yarik/GIT/greenplum-dwh/liquibase/sql_scripts/baseline/ddl"
changelog_path = "/Users/avotech/yarik/GIT/greenplum-dwh/liquibase/DE_changelogs/baseline"
changelog_file = os.path.join(changelog_path, "changelog.xml")


def create_baseline_changelog(sql_scripts_path, changelog_file):
    # Ensure the changelog directory exists
    os.makedirs(changelog_path, exist_ok=True)

    # Define namespaces for Liquibase XML schema
    namespaces = {
        None: "http://www.liquibase.org/xml/ns/dbchangelog",  # Default namespace
        "xsi": "http://www.w3.org/2001/XMLSchema-instance"
    }

    # Create the root element for the XML
    root = etree.Element("databaseChangeLog", nsmap=namespaces)
    root.set(
        "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation",
        "http://www.liquibase.org/xml/ns/dbchangelog "
        "http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd"
    )

    # Iterate through all SQL files in the directory and add to changelog
    for root_dir, _, files in os.walk(sql_scripts_path):
        for file_name in files:
            if file_name.endswith(".sql"):
                file_path = os.path.join(root_dir, file_name)
                relative_path = os.path.relpath(file_path, start=os.path.dirname(changelog_path))
                relative_path = relative_path.replace('..','liquibase')

                changeset_id = f"baseline-{os.path.relpath(file_path, sql_scripts_path)}"

                changeset = etree.SubElement(root, "changeSet", id=changeset_id, author="baseline",
                                             runOnChange="false", runAlways="false")

                # Add sqlFile element
                sqlfile = etree.SubElement(changeset, "sqlFile", {
                    "path": relative_path,
                    "splitStatements": "true",
                    "endDelimiter": ";"
                })


    # Write the XML structure to the changelog file
    tree = etree.ElementTree(root)
    with open(changelog_file, "wb") as f:
        tree.write(f, pretty_print=True, xml_declaration=True, encoding="UTF-8")

    print(f"Baseline changelog created at {changelog_file}")


# Run the function to create baseline changelog
create_baseline_changelog(sql_scripts_path, changelog_file)
