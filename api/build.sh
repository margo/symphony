#!/bin/bash

set -e  # Exit on any error

# Remove existing binary if present
rm -f symphony-api


# Build Rust provider
pushd .
cd pkg/apis/v1alpha1/providers/target/rust
echo "Building Rust provider..."
cargo build --release
popd  # back to the api folder

# Build Go binary
echo "Building symphony-api..."
rm -rf vendor
go build -o symphony-api

# Replace binary in target location
echo "Replacing symphony-api in ~/margo/binary/wfm/..."
rm -f ~/margo/binary/wfm/symphony-api
cp symphony-api ~/margo/binary/wfm/symphony-api

echo "Build and deployment completed successfully!"