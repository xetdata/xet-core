// // use crate::config::UserIdType;
// use crate::config::XetConfig;
// use chrono::{NaiveDateTime, Utc};
// use prometheus_dict_encoder::DictEncoder;
// use reqwest::Client;
// use serde::{Deserialize, Serialize};
// use serde_json::Value;
// use std::{collections::HashMap, time::Duration};
// use sysinfo::{System, SystemExt};
// use xet_error::Error;
// 
// #[derive(Error, Debug)]
// pub enum AxeError {
//     #[error("initialization error")]
//     InitializationError(String),
// 
//     #[error("A network error: {0}")]
//     NetworkError(#[from] reqwest::Error),
// 
//     #[error("Serde error")]
//     SerdeError(#[from] serde_json::Error),
// 
//     #[error("unknown error")]
//     UnknownError,
// }
// 
// const API_ENDPOINT: &str = "https://app.posthog.com/capture/";
// const TIMEOUT: u64 = 800;
// 
// #[derive(Serialize, Deserialize, Debug, Clone)]
// struct UploadBody {
//     pub api_key: String,
//     pub event: String,
//     pub properties: HashMap<String, Value>,
//     timestamp: Option<NaiveDateTime>,
// }
// 
// #[derive(Clone, Debug)]
// pub struct Axe {
//     client: Option<Client>,
//     upload_body: Option<UploadBody>,
// }
// 
// impl Axe {
//     fn initialize_axe_upload(
//         command_name: &str,
//         cfg: &XetConfig,
//         event_properties: Option<HashMap<String, f64>>,
//     ) -> UploadBody {
//         let mut upload_body = UploadBody {
//             api_key: cfg.axe.axe_code.clone(),
//             event: command_name.to_string(),
//             properties: HashMap::new(),
//             timestamp: Some(Utc::now().naive_utc()),
//         };
//         if let Some(event_properties) = event_properties {
//             for (k, v) in event_properties.iter() {
//                 upload_body
//                     .properties
//                     .insert(k.to_string(), Value::String(v.to_string()));
//             }
//         }
//         for (i, xetea_url) in cfg.known_remote_repo_paths().iter().enumerate() {
//             upload_body.properties.insert(
//                 if i == 0 {
//                     // common case
//                     "remote_url".to_string()
//                 } else {
//                     format!("remote_url_{i}")
//                 },
//                 Value::String(xetea_url.to_string()),
//             );
//         }
// 
//         // add user id info properties
//         let (user_id, user_id_type) = cfg.user.get_user_id();
//         match user_id_type {
//             UserIdType::SSH => {
//                 upload_body
//                     .properties
//                     .insert("user_ssh".to_string(), Value::String(user_id.to_string()));
//             }
//             UserIdType::HTTPS => {
//                 upload_body
//                     .properties
//                     .insert("user_https".to_string(), Value::String(user_id.to_string()));
//             }
//             UserIdType::UNKNOWN => { /* don't add specific user type info */ }
//         }
//         // add user distinct id property
//         upload_body
//             .properties
//             .insert("distinct_id".to_string(), Value::String(user_id));
// 
//         if let Some(user_email) = cfg.user.email.clone() {
//             // property name needs to be "email" exactly for PostHog's Hubspot app to sync data properly
//             // For more details see: https://github.com/PostHog/hubspot-plugin
//             upload_body
//                 .properties
//                 .insert("email".to_string(), Value::String(user_email));
//         }
//         if let Some(login_id) = cfg.user.login_id.clone() {
//             if !login_id.is_empty() {
//                 upload_body
//                     .properties
//                     .insert("login_id".to_string(), Value::String(login_id));
//             }
//         }
//         if let Some(user_owner) = cfg.user.owner.clone() {
//             upload_body
//                 .properties
//                 .insert("user_owner".to_string(), Value::String(user_owner));
//         }
// 
//         upload_body.properties.insert(
//             "git_xet_version".to_string(),
//             Value::String(crate::constants::CURRENT_VERSION.to_string()),
//         );
//         // add repo paths
//         upload_body.properties.insert(
//             "repo_paths".to_string(),
//             Value::String(
//                 serde_json::to_string(&cfg.known_remote_repo_paths())
//                     .unwrap_or_else(|_| "[]".to_string()),
//             ),
//         );
//         upload_body
//     }
// 
//     fn enrich_upload_system_metrics(upload_body: &mut UploadBody) {
//         let mut sys = System::default();
//         sys.refresh_memory();
//         sys.refresh_cpu();
// 
//         if let Some(sys_name) = sys.name() {
//             upload_body
//                 .properties
//                 .insert("sys_name".to_string(), Value::String(sys_name));
//         }
//         if let Some(sys_kernel_version) = sys.kernel_version() {
//             upload_body.properties.insert(
//                 "sys_kernel_version".to_string(),
//                 Value::String(sys_kernel_version),
//             );
//         }
//         if let Some(os_version) = sys.os_version() {
//             upload_body
//                 .properties
//                 .insert("sys_os_version".to_string(), Value::String(os_version));
//         }
//         upload_body.properties.insert(
//             "sys_total_memory".to_string(),
//             Value::String(sys.total_memory().to_string()),
//         );
//         upload_body.properties.insert(
//             "sys_used_memory".to_string(),
//             Value::String(sys.used_memory().to_string()),
//         );
//         upload_body.properties.insert(
//             "sys_number_cpus".to_string(),
//             Value::String(sys.cpus().len().to_string()),
//         );
//     }
// 
//     pub async fn command_start(command_name: &str, cfg: &XetConfig) -> Result<Self, AxeError> {
//         if !cfg.axe.enabled() || command_name.starts_with("hooks") {
//             return Ok(Self {
//                 client: None,
//                 upload_body: None,
//             });
//         }
//         let client = reqwest::Client::builder()
//             .timeout(Duration::from_millis(TIMEOUT))
//             .build()?;
//         let mut upload_body = Axe::initialize_axe_upload(command_name, cfg, None);
//         Axe::enrich_upload_system_metrics(&mut upload_body);
//         upload_body
//             .properties
//             .insert("edge".to_string(), Value::String("start".to_string()));
// 
//         // ignore errors for now
//         let client_clone = client.clone();
//         let upload_body_clone = upload_body.clone();
//         tokio::task::spawn(async move {
//             client_clone
//                 .post(API_ENDPOINT)
//                 .json(&upload_body_clone)
//                 .send()
//                 .await
//         });
//         Ok(Self {
//             client: Some(client),
//             upload_body: Some(upload_body),
//         })
//     }
//     pub async fn send_metrics(
//         command_name: &str,
//         cfg: &XetConfig,
//         event_properties: Option<HashMap<String, f64>>,
//         background: bool,
//     ) -> Result<bool, AxeError> {
//         if !cfg.axe.enabled() {
//             return Ok(false);
//         }
//         let client = reqwest::Client::builder()
//             .timeout(Duration::from_millis(TIMEOUT))
//             .build()?;
//         let upload_body = Axe::initialize_axe_upload(command_name, cfg, event_properties);
//         if background {
//             // ignore errors for now
//             let client_clone = client.clone();
//             let upload_body_clone = upload_body.clone();
//             tokio::task::spawn(async move {
//                 client_clone
//                     .post(API_ENDPOINT)
//                     .json(&upload_body_clone)
//                     .send()
//                     .await
//             });
//         } else {
//             let _ = client.post(API_ENDPOINT).json(&upload_body).send().await;
//         }
//         Ok(true)
//     }
// 
//     pub async fn command_complete(&mut self) {
//         if let (Some(mut upload_body), Some(client)) = (self.upload_body.clone(), &self.client) {
//             upload_body.event.push_str("Complete");
//             upload_body
//                 .properties
//                 .insert("edge".to_string(), Value::String("complete".to_string()));
//             if let Ok(metrics) = DictEncoder::new().encode(&prometheus::gather()) {
//                 for (k, v) in metrics.iter() {
//                     upload_body
//                         .properties
//                         .insert(k.to_string(), Value::String(v.to_string()));
//                 }
//             }
//             let _ = client.post(API_ENDPOINT).json(&upload_body).send().await;
//         }
//     }
// }
