
#
# Default.
#

default: build

#
# Tasks.
#

# Create schema.
schema:
	@node generate-schema.js

# Build.
build: 
	@pip3 install .

# Dev.
dev:
	@python3 setup.py develop

# Deploy.
release:
	@python3 setup.py sdist upload

# Test.
test:
	@python3 tests/test_tap_toast.py

#
# Phonies.
#

.PHONY: build
.PHONY: dev
.PHONY: release
.PHONY: schema
.PHONY: test

