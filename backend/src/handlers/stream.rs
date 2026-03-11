use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Handler, Message, StreamHandler,
    WrapFuture,
};
use actix_web::{Error, HttpRequest, HttpResponse, get, web};
use actix_web_actors::ws;
use serde_json::json;

use crate::models::{AppState, DashboardStreamQuery};
use crate::validation::{validated_limit, validated_minutes};

use super::analytics::analytics_dashboard_payload;

const HEARTBEAT_SECONDS: u64 = 15;

#[get("/ws/dashboard")]
pub async fn ws_dashboard(
    req: HttpRequest,
    stream: web::Payload,
    query: web::Query<DashboardStreamQuery>,
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    state.metrics.inc_request();

    let minutes = match validated_minutes(&state, "minutes", query.minutes, 1440, 1, 10080) {
        Ok(v) => v,
        Err(resp) => return Ok(resp),
    };
    let limit = match validated_limit(&state, "limit", query.limit, 10, 1, 100) {
        Ok(v) => v,
        Err(resp) => return Ok(resp),
    };
    let actor = DashboardStreamSession {
        state,
        minutes,
        limit,
        snapshot_seq: 0,
        last_anchor_unix_ms: 0,
    };

    ws::start(actor, &req, stream)
}

struct DashboardStreamSession {
    state: web::Data<AppState>,
    minutes: u32,
    limit: usize,
    snapshot_seq: u64,
    last_anchor_unix_ms: u64,
}

#[derive(Message)]
#[rtype(result = "()")]
struct DashboardRefresh {
    anchor_unix_ms: u64,
}

impl DashboardStreamSession {
    fn send_snapshot(&mut self, ctx: &mut ws::WebsocketContext<Self>, anchor_unix_ms: Option<u64>) {
        self.snapshot_seq = self.snapshot_seq.saturating_add(1);
        let snapshot_seq = self.snapshot_seq;
        let state = self.state.clone();
        let minutes = self.minutes;
        let limit = self.limit;
        let resolved_anchor = anchor_unix_ms.unwrap_or_else(crate::utils::now_unix_ms);
        self.last_anchor_unix_ms = self.last_anchor_unix_ms.max(resolved_anchor);
        let resolved_anchor = self.last_anchor_unix_ms;

        ctx.spawn(
            async move {
                match analytics_dashboard_payload(&state, minutes, limit, resolved_anchor).await {
                    Ok(payload) => json!({
                        "type": "snapshot",
                        "payload": payload
                    }),
                    Err(_) => json!({
                        "type": "error",
                        "message": "dashboard snapshot failed"
                    }),
                }
            }
            .into_actor(self)
            .map(move |message, actor, ctx| {
                if snapshot_seq == actor.snapshot_seq {
                    ctx.text(message.to_string());
                }
            }),
        );
    }

    fn spawn_signal_forwarder(state: web::Data<AppState>, addr: Addr<Self>) {
        let mut rx = state.dashboard_tx.subscribe();
        actix_web::rt::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(anchor_unix_ms) => {
                        addr.do_send(DashboardRefresh { anchor_unix_ms });
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
}

impl Actor for DashboardStreamSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.send_snapshot(ctx, None);
        Self::spawn_signal_forwarder(self.state.clone(), ctx.address());
        ctx.run_interval(Duration::from_secs(HEARTBEAT_SECONDS), |_actor, ctx| {
            ctx.ping(b"hb");
        });
    }
}

impl Handler<DashboardRefresh> for DashboardStreamSession {
    type Result = ();

    fn handle(&mut self, message: DashboardRefresh, ctx: &mut Self::Context) -> Self::Result {
        self.send_snapshot(ctx, Some(message.anchor_unix_ms));
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for DashboardStreamSession {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(ws::Message::Ping(bytes)) => ctx.pong(&bytes),
            Ok(ws::Message::Pong(_)) => {}
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Ok(ws::Message::Text(_))
            | Ok(ws::Message::Binary(_))
            | Ok(ws::Message::Continuation(_))
            | Ok(ws::Message::Nop) => {}
            Err(_) => ctx.stop(),
        }
    }
}
