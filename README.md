![Calagopus Logo](https://calagopus.com/fulllogo.svg)

# Tundra

[![Rust](https://img.shields.io/badge/rust-stable-orange.svg?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/github/license/calagopus/tundra?color=blue)](https://github.com/calagopus/tundra/blob/main/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/calagopus/tundra)](https://github.com/calagopus/tundra/issues)
[![GitHub stars](https://img.shields.io/github/stars/calagopus/tundra)](https://github.com/calagopus/tundra/stargazers)
[![Discord](https://img.shields.io/discord/1429911351777824892?label=discord&logo=discord&color=5865F2)](https://discord.gg/uSM8tvTxBV)

tundra is a private networking layer between game server nodes, written in Rust. Containers
on different hosts reach each other through loopback addresses over one mTLS QUIC connection
per node pair - TCP over streams, UDP over datagrams - with a control plane as the single
source of truth for identity, membership and access. Planned restarts exec in place and
carry every open connection across, so a node upgrade drops nothing.

## Star History

![Star History Chart](https://api.star-history.com/chart?repos=calagopus/tundra&type=date&legend=top-left&sealed_token=B2O-QGHUHAa_2R6TXAtmVmA-ASHkIyhBD3Rm6jlD9mOeO9XJsHW0uBvsZ-5zINucUHJPH5c29w8c7lL_2Kr7tb5770-KK58lG2pGrET0ksegRMrP1IEbft05EdOtyO6RAUCo1FCK5gnNscF6lwXhRp5LLQd08n2sZgUisdnct1irxGRvQmzUx9o-Bk4o)

## Incus runtime

The node can discover local Incus game containers instead of Docker. Set `WINGS_INCUS_SOCKET`, `WINGS_INCUS_PROJECT`, and `WINGS_INCUS_OWNER` to select Incus discovery. It admits only owned `wgs-UUID` containers, inspects their host PID and private address, and polls for starts, stops, and PID changes. Private TCP/UDP listeners, QUIC transport, and ACL enforcement stay shared with the Docker runtime.

The `tundra-node` library exports `run()` for Wings to supervise an embedded node process. The standalone executable uses the same entry point.
