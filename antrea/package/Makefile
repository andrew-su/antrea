ROOT_DIR:=$(PWD)
SCRIPTS_DIR := $(ROOT_DIR)/hack/scripts
PACKAGES_SCRIPTS_DIR := $(SCRIPTS_DIR)/packages
ARTIFACTS_DIR := "./artifacts"
VERSION="2.1.0"

generate-openapi-schema-package: ## Generate package with OpenAPI v3 schema. Usage make generate-openapi-schema-package  VERSION=1.22.4
	@printf "\n===> Generating OpenAPIv3 schema for $${VERSION}\n";\
	$(PACKAGES_SCRIPTS_DIR)/check-sample-values-and-render-ytt.sh  $(VERSION) \
	&& mkdir -p ${ARTIFACTS_DIR} \
	&& cd ${ARTIFACTS_DIR} \
	&& ytt -f ../bundle/config/_ytt_lib/bundle/config/schema.yaml --data-values-schema-inspect -o openapi-v3 > openapi-schema.yaml \
	&& ytt -f ../upstream-package.yaml -f $(PACKAGES_SCRIPTS_DIR)/package-overlay.yaml --data-value-file openapi=openapi-schema.yaml > generated-package.yaml \
	&& mv generated-package.yaml ../upstream-package.yaml
	@printf "===> upstream-package.yaml has been updated with openAPIv3 schema in its valuesSchema field for $${VERSION}\n";
