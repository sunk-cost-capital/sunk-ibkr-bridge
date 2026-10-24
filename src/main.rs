//! Command-line entry point for the Sunk IBKR browser bridge.

use rand::RngCore;
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use sunk_ibkr_bridge::{
    BridgeConfig, DEFAULT_BRIDGE_CLIENT_ID, GatewayConfig, SecurityConfig, parse_gateway_address,
};

const DEFAULT_ORIGIN: &str = "https://sunkcostcapital.com";

#[derive(Debug)]
struct Args {
    listen: SocketAddr,
    gateway_address: Option<SocketAddr>,
    client_id: Option<i32>,
    allowed_origins: Vec<String>,
    token: Option<String>,
}

impl Args {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut listen = "127.0.0.1:17654".parse()?;
        let mut gateway_address = None;
        let mut client_id = None;
        let mut allowed_origins = Vec::new();
        let mut token = env::var("SUNK_IBKR_BRIDGE_TOKEN").ok();
        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            let value = match argument.as_str() {
                "--help" | "-h" => {
                    println!(
                        "sunk-ibkr-bridge {}\n\n--listen ADDRESS\n--gateway-address ADDRESS\n--client-id ID\n--allow-origin ORIGIN (repeatable)\n--token TOKEN (or SUNK_IBKR_BRIDGE_TOKEN)",
                        env!("CARGO_PKG_VERSION")
                    );
                    std::process::exit(0);
                }
                "--version" | "-V" => {
                    println!("sunk-ibkr-bridge {}", env!("CARGO_PKG_VERSION"));
                    std::process::exit(0);
                }
                "--listen" | "--gateway-address" | "--client-id" | "--allow-origin" | "--token" => {
                    arguments
                        .next()
                        .ok_or_else(|| format!("{argument} requires a value"))?
                }
                _ => return Err(format!("unknown argument: {argument}").into()),
            };
            match argument.as_str() {
                "--listen" => listen = value.parse()?,
                "--gateway-address" => gateway_address = Some(parse_gateway_address(&value)?),
                "--client-id" => client_id = Some(value.parse()?),
                "--allow-origin" => allowed_origins.push(value),
                "--token" => token = Some(value),
                _ => unreachable!("help and version exit above"),
            }
        }
        if allowed_origins.is_empty() {
            allowed_origins.push(DEFAULT_ORIGIN.to_string());
        }
        Ok(Self {
            listen,
            gateway_address,
            client_id,
            allowed_origins,
            token,
        })
    }
}

fn generated_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sunk_ibkr_bridge=info".into()),
        )
        .init();

    let args = Args::parse()?;
    if !args.listen.ip().is_loopback() {
        return Err("--listen must use a loopback address".into());
    }
    if args.listen.ip() == IpAddr::V4(Ipv4Addr::UNSPECIFIED) {
        return Err("--listen cannot use an unspecified address".into());
    }

    let token = args.token.unwrap_or_else(|| {
        let token = generated_token();
        eprintln!(
            "Generated temporary bridge token (copy it into the Sunk web connection):\n{token}"
        );
        token
    });
    let gateway = GatewayConfig::from_env(DEFAULT_BRIDGE_CLIENT_ID)?;
    let gateway = if args.gateway_address.is_some() || args.client_id.is_some() {
        GatewayConfig::new(
            args.gateway_address
                .map(|address| vec![address])
                .unwrap_or_else(|| gateway.addresses().to_vec()),
            args.client_id.unwrap_or(gateway.client_id()),
        )?
    } else {
        gateway
    };
    let config = BridgeConfig {
        listen: args.listen,
        gateway,
        security: SecurityConfig::new(token, args.allowed_origins)?,
    };
    sunk_ibkr_bridge::run(config).await?;
    Ok(())
}
