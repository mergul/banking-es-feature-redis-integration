use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebSocketMessage {
    // Client to Server
    Subscribe {
        username: String,
    },
    Unsubscribe {
        username: String,
    },

    // Server to Client
    ProjectionUpdated {
        username: String,
        account_id: String,
        balance: String,
        is_active: bool,
    },
    AccountCreated {
        username: String,
        account_id: String,
        initial_balance: String,
    },
    Error {
        message: String,
    },
}

#[derive(Debug)]
pub struct WebSocketManager {
    connections: Arc<DashMap<String, broadcast::Sender<WebSocketMessage>>>,
}

impl WebSocketManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
        }
    }

    pub fn subscribe(&self, username: String) -> broadcast::Receiver<WebSocketMessage> {
        let (tx, rx) = broadcast::channel(100);
        self.connections.insert(username.clone(), tx);
        info!("WebSocket subscription created for user: {}", username);
        rx
    }

    pub fn unsubscribe(&self, username: &str) {
        self.connections.remove(username);
        info!("WebSocket subscription removed for user: {}", username);
    }

    pub fn broadcast_to_user(&self, username: &str, message: WebSocketMessage) {
        if let Some(tx) = self.connections.get(username) {
            if let Err(e) = tx.send(message.clone()) {
                warn!("Failed to broadcast message to user {}: {}", username, e);
            } else {
                info!("Message broadcasted to user {}: {:?}", username, message);
            }
        } else {
            warn!("No WebSocket connection found for user: {}", username);
        }
    }

    pub fn broadcast_account_created(
        &self,
        username: &str,
        account_id: &str,
        initial_balance: &str,
    ) {
        let message = WebSocketMessage::AccountCreated {
            username: username.to_string(),
            account_id: account_id.to_string(),
            initial_balance: initial_balance.to_string(),
        };
        self.broadcast_to_user(username, message);
    }

    pub fn broadcast_projection_updated(
        &self,
        username: &str,
        account_id: &str,
        balance: &str,
        is_active: bool,
    ) {
        let message = WebSocketMessage::ProjectionUpdated {
            username: username.to_string(),
            account_id: account_id.to_string(),
            balance: balance.to_string(),
            is_active,
        };
        self.broadcast_to_user(username, message);
    }
}

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    axum::extract::State(ws_manager): axum::extract::State<Arc<WebSocketManager>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, ws_manager))
}

async fn handle_socket(socket: WebSocket, ws_manager: Arc<WebSocketManager>) {
    let (mut sender, mut receiver) = socket.split();
    let mut username: Option<String> = None;
    let mut rx: Option<broadcast::Receiver<WebSocketMessage>> = None;

    // Handle incoming messages
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                match serde_json::from_str::<WebSocketMessage>(&text) {
                    Ok(WebSocketMessage::Subscribe { username: user }) => {
                        info!("WebSocket subscription request for user: {}", user);
                        username = Some(user.clone());
                        let subscription_rx = ws_manager.subscribe(user.clone());
                        rx = Some(subscription_rx);

                        // Send confirmation with proper format
                        let response = json!({
                            "type": "ProjectionUpdated",
                            "data": {
                                "username": user,
                                "account_id": "",
                                "balance": "",
                                "is_active": true
                            }
                        });
                        if let Ok(text) = serde_json::to_string(&response) {
                            if let Err(e) = sender.send(Message::Text(text.into())).await {
                                error!("Failed to send subscription confirmation: {}", e);
                                break;
                            }
                        }
                    }
                    Ok(WebSocketMessage::Unsubscribe { username: user }) => {
                        info!("WebSocket unsubscription request for user: {}", user);
                        ws_manager.unsubscribe(&user);
                        if username.as_ref() == Some(&user) {
                            username = None;
                            rx = None;
                        }
                    }
                    Ok(_) => {
                        // Handle other message types
                    }
                    Err(e) => {
                        // Try parsing as client format with type and data fields
                        match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(json_value) => {
                                if let (Some(msg_type), Some(data)) = (
                                    json_value.get("type").and_then(|v| v.as_str()),
                                    json_value.get("data"),
                                ) {
                                    match msg_type {
                                        "Subscribe" => {
                                            if let Some(user) =
                                                data.get("username").and_then(|v| v.as_str())
                                            {
                                                info!(
                                                    "WebSocket subscription request for user: {}",
                                                    user
                                                );
                                                username = Some(user.to_string());
                                                let subscription_rx =
                                                    ws_manager.subscribe(user.to_string());
                                                rx = Some(subscription_rx);

                                                // Send confirmation
                                                let response = json!({
                                                    "type": "ProjectionUpdated",
                                                    "data": {
                                                        "username": user,
                                                        "account_id": "",
                                                        "balance": "",
                                                        "is_active": true
                                                    }
                                                });
                                                if let Ok(text) = serde_json::to_string(&response) {
                                                    if let Err(e) = sender
                                                        .send(Message::Text(text.into()))
                                                        .await
                                                    {
                                                        error!("Failed to send subscription confirmation: {}", e);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        "Unsubscribe" => {
                                            if let Some(user) =
                                                data.get("username").and_then(|v| v.as_str())
                                            {
                                                info!(
                                                    "WebSocket unsubscription request for user: {}",
                                                    user
                                                );
                                                ws_manager.unsubscribe(user);
                                                if username.as_ref() == Some(&user.to_string()) {
                                                    username = None;
                                                    rx = None;
                                                }
                                            }
                                        }
                                        _ => {
                                            warn!("Unknown WebSocket message type: {}", msg_type);
                                        }
                                    }
                                } else {
                                    error!(
                                        "Invalid WebSocket message format: missing type or data"
                                    );
                                }
                            }
                            Err(_) => {
                                error!("Failed to parse WebSocket message: {}", e);
                                let error_msg = WebSocketMessage::Error {
                                    message: format!("Invalid message format: {}", e),
                                };
                                if let Ok(text) = serde_json::to_string(&error_msg) {
                                    let _ = sender.send(Message::Text(text.into())).await;
                                }
                            }
                        }
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("WebSocket connection closed for user: {:?}", username);
                break;
            }
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    // Cleanup
    if let Some(user) = username {
        ws_manager.unsubscribe(&user);
        info!("WebSocket connection cleaned up for user: {}", user);
    }
}

// Helper function to get the next message from receiver
async fn next_message(
    receiver: &mut broadcast::Receiver<WebSocketMessage>,
) -> Option<WebSocketMessage> {
    match receiver.recv().await {
        Ok(msg) => Some(msg),
        Err(broadcast::error::RecvError::Closed) => None,
        Err(broadcast::error::RecvError::Lagged(_)) => {
            warn!("WebSocket receiver lagged, skipping messages");
            None
        }
    }
}
