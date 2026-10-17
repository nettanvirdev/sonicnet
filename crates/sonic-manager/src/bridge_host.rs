//! What the browser bridge asks of the app: probing a link the way `Command::Probe` does (with the
//! browser's sign-in), and handing confirmed links to the manager.

use std::sync::Arc;

use sonic_bridge::{BridgeHost, OfferedLink};
use sonic_core::{BrowserContext, ProbeResult};
use tokio::sync::mpsc;

use crate::{Ctx, probe_link};

/// What the bridge tells the manager.
#[derive(Debug)]
pub(crate) enum BridgeEvent {
    Offer(Box<OfferedLink>),
    ExtensionSeen,
}

#[derive(Debug)]
pub(crate) struct BridgeLink {
    ctx: Arc<Ctx>,
    tx: mpsc::UnboundedSender<BridgeEvent>,
}

impl BridgeLink {
    pub(crate) fn new(ctx: Arc<Ctx>, tx: mpsc::UnboundedSender<BridgeEvent>) -> Self {
        Self { ctx, tx }
    }
}

impl BridgeHost for BridgeLink {
    fn probe(
        &self,
        url: String,
        browser: Option<BrowserContext>,
    ) -> impl Future<Output = Result<ProbeResult, String>> + Send {
        let networks = self.ctx.networks.clone();
        let tuning = self.ctx.tuning.clone();
        async move { probe_link(&networks, &tuning, &url, browser.as_ref()).await }
    }

    fn offer(&self, link: OfferedLink) {
        let _ = self.tx.send(BridgeEvent::Offer(Box::new(link)));
    }

    fn extension_seen(&self) {
        let _ = self.tx.send(BridgeEvent::ExtensionSeen);
    }
}
