import os

source_dir = "/Users/avotech/yarik/GIT/greenplum-dwh/ddl/"
target_dir = "/Users/avotech/yarik/GIT/greenplum-dwh/liquibase/ddl/"

liquibase_header_template = """--liquibase formatted sql\n
--changeset baseline:#name# labels:skip
"""

for root, dirs, files in os.walk(source_dir):
    relative_path = os.path.relpath(root, source_dir)
    target_path = os.path.join(target_dir, relative_path)
    os.makedirs(target_path, exist_ok=True)

    for filename in files:
        src_file_path = os.path.join(root, filename)
        tgt_file_path = os.path.join(target_path, filename)

        name_without_ext = os.path.splitext(os.path.basename(filename))[0]

        liquibase_header = liquibase_header_template.replace("#name#", name_without_ext)

        with open(src_file_path, 'r') as src_file:
            file_content = src_file.read()

        final_content = liquibase_header + file_content

        with open(tgt_file_path, 'w') as tgt_file:
            tgt_file.write(final_content)

        print(f"Processed file: {src_file_path} -> {tgt_file_path}")
