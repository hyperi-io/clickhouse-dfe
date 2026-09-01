# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- TCP transport (connection actor, deadpool pool, retry, TLS trust) and the Native-format wire codec
- Column-typed reads over TCP: `TcpClient::query(sql).fetch_blocks()` plus `DecodedBlock::column_as` over the `FromColumn` trait
