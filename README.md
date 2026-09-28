# sunk-ibkr-bridge

Release downloads for the Sunk Cost Capital IBKR bridge: a loopback-only bridge that connects the Sunk web app to your local IB Gateway / TWS socket API.

- Download the latest build for your platform from the [Releases](https://github.com/sunk-cost-capital/sunk-ibkr-bridge/releases/latest) page.
- The bridge only listens on localhost and only accepts explicitly allowed web origins.
- Source and issue tracking live in the Sunk monorepo (`packages/ibkr-web-bridge`).

## Protocol versions

| Bridge release | Protocol | Notes |
| --- | --- | --- |
| 0.2.x | v3 | Required by the web app from the v3 rollout onward. Older bridges are refused with an update prompt. |
| 0.1.x | v2 | Legacy. Update to 0.2.x. |
