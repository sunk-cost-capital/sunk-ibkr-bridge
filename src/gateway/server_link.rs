//! IB Gateway's link to IBKR's servers, read from TWS system messages.
//!
//! IB Gateway keeps answering API requests from its own cache after it loses
//! its connection to IBKR's servers, so a successful account-state read does
//! not prove the account is live. TWS reports the link through system
//! messages sent to every API client (request id -1):
//!
//! - `1100` "Connectivity between IB and Trader Workstation has been lost."
//! - `1101` "...has been restored - data lost." and `1102` "...has been
//!   restored - data maintained."
//! - `2110` "Connectivity between Trader Workstation and server is broken.
//!   It will be restored automatically."
//!
//! IBKR documents no message that clears `2110` on its own; a restored link
//! is announced by `1101`/`1102` or by the farm-OK messages `2104`, `2106`
//! and `2158`, which therefore clear a `2110` outage but never a `1100` one.
//! Sources: TWS API [message codes](https://interactivebrokers.github.io/tws-api/message_codes.html)
//! and IBKR Campus [system message codes](https://www.interactivebrokers.com/docs/tws-api/doc/error-handling/system-message-codes).
//!
//! Messages sent before the watch subscribes (it starts with each new
//! connection) are not seen, so a link that was already down when the
//! connection opened reads as connected until IBKR next reports it.

use crate::contract::ServerLink;
use ibapi::prelude::Client;
use std::sync::{Arc, Mutex, Weak};
use tokio::task::JoinHandle;

/// TWS: connectivity between IB and TWS has been lost.
const LINK_LOST: i32 = 1100;
/// TWS: connectivity between TWS and the server is broken.
const SERVER_BROKEN: i32 = 2110;
/// TWS: connectivity restored, with market data lost (1101) or kept (1102).
const LINK_RESTORED: [i32; 2] = [1101, 1102];
/// TWS: a market-data, historical-data or security-definition farm is OK.
const FARM_OK: [i32; 3] = [2104, 2106, 2158];

/// Link state kept between system messages.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum LinkState {
    /// No outage reported.
    #[default]
    Connected,
    /// TWS reported `1100`; only `1101`/`1102` restore it.
    Lost,
    /// TWS reported `2110`; a restore or farm-OK message clears it.
    ServerBroken,
}

impl LinkState {
    /// The state after one TWS message code.
    ///
    /// # Arguments
    ///
    /// * `code` - TWS message code of an unrouted notice.
    ///
    /// # Returns
    ///
    /// The next state; unrelated codes leave it unchanged.
    pub(super) fn after(self, code: i32) -> Self {
        match code {
            LINK_LOST => Self::Lost,
            SERVER_BROKEN if self == Self::Lost => Self::Lost,
            SERVER_BROKEN => Self::ServerBroken,
            code if LINK_RESTORED.contains(&code) => Self::Connected,
            code if FARM_OK.contains(&code) && self == Self::ServerBroken => Self::Connected,
            _ => self,
        }
    }

    /// The link as reported to callers.
    pub(super) fn link(self) -> ServerLink {
        match self {
            Self::Connected => ServerLink::Connected,
            Self::Lost | Self::ServerBroken => ServerLink::Lost,
        }
    }
}

/// Watches one TWS client's system messages for the server link.
#[derive(Debug)]
pub(super) struct ServerLinkWatch {
    client: Weak<Client>,
    state: Arc<Mutex<LinkState>>,
    task: JoinHandle<()>,
}

impl ServerLinkWatch {
    /// Start watching a newly connected client.
    ///
    /// # Arguments
    ///
    /// * `client` - The client whose notices are read.
    ///
    /// # Returns
    ///
    /// The watch, or none when the client offers no notice stream.
    pub(super) fn start(client: &Arc<Client>) -> Option<Self> {
        let mut notices = client.notice_stream().ok()?;
        let state = Arc::new(Mutex::new(LinkState::default()));
        let shared = Arc::clone(&state);
        let task = tokio::spawn(async move {
            while let Some(notice) = notices.next().await {
                let Ok(mut state) = shared.lock() else { return };
                let next = state.after(notice.code);
                if next != *state {
                    tracing::info!(
                        code = notice.code,
                        link = ?next.link(),
                        "IB Gateway server link changed"
                    );
                    *state = next;
                }
            }
        });
        Some(Self {
            client: Arc::downgrade(client),
            state,
            task,
        })
    }

    /// The link of `client`, or none when this watch follows another client.
    pub(super) fn link_of(&self, client: &Arc<Client>) -> Option<ServerLink> {
        let watched = self.client.upgrade()?;
        if !Arc::ptr_eq(&watched, client) {
            return None;
        }
        self.state.lock().ok().map(|state| state.link())
    }
}

impl Drop for ServerLinkWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replay(codes: &[i32]) -> LinkState {
        codes
            .iter()
            .fold(LinkState::default(), |state, code| state.after(*code))
    }

    #[test]
    fn a_lost_link_needs_tws_to_report_it_restored() {
        assert_eq!(replay(&[1100]).link(), ServerLink::Lost);
        // Farms reporting OK do not prove the main link is back.
        assert_eq!(replay(&[1100, 2104, 2106, 2158]).link(), ServerLink::Lost);
        assert_eq!(replay(&[1100, 2110]).link(), ServerLink::Lost);
        assert_eq!(replay(&[1100, 1101]).link(), ServerLink::Connected);
        assert_eq!(replay(&[1100, 1102]).link(), ServerLink::Connected);
    }

    #[test]
    fn a_broken_server_connection_clears_on_restore_or_farm_ok() {
        assert_eq!(replay(&[2110]).link(), ServerLink::Lost);
        assert_eq!(replay(&[2110, 2104]).link(), ServerLink::Connected);
        assert_eq!(replay(&[2110, 1102]).link(), ServerLink::Connected);
        // A later 1100 outranks the 2110 it follows.
        assert_eq!(replay(&[2110, 1100, 2104]).link(), ServerLink::Lost);
    }

    #[test]
    fn unrelated_messages_leave_the_link_alone() {
        assert_eq!(
            replay(&[2104, 2107, 2108, 399]).link(),
            ServerLink::Connected
        );
        assert_eq!(replay(&[1100, 2107, 202]).link(), ServerLink::Lost);
    }
}
