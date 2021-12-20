
echo "===== Compile antrea e2e testcases ======"
compile_e2e "standard"

echo "====== Generating version Files for CI and Consumers ======"
publish_version_files
