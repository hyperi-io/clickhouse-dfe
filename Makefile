# Project:   clickhouse-dfe
# File:      Makefile
# Purpose:   CI targets wrapping hyperi-ci
#
# License:   Apache-2.0
# Copyright: (c) 2026 HYPERI PTY LIMITED

.PHONY: check quality test build check-features

# The full local pre-push gate -- the same suite CI runs.
check:
	hyperi-ci check

quality:
	hyperi-ci run quality

test:
	hyperi-ci run test

build:
	hyperi-ci run build

# Both ends of the feature matrix. Every layer is optional, so a change that
# only compiles with the default set is a broken change.
check-features:
	cargo check --all-features
	cargo check --no-default-features
