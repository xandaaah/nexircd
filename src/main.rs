use std::{collections::HashMap, str::FromStr};

use anyhow::anyhow;
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::{TcpListener, TcpStream}, select, sync::broadcast};

#[derive(Clone)]
enum GlobalMessage {
    SendMessage {from: String, msg: String, targets: Vec<String>},
    WallopsMessage {message: String}
}

enum IRCMessage {
    // Numbered responses
    /// RPL_WELCOME - 001
    RplWelcome {client_nick: String, msg: String},
    /// RPL_YOURHOST - 002
    RplYourhost {client_nick: String, msg: String}, 
    /// RPL_CREATED - 003
    RplCreated {client_nick: String, msg: String}, 
    /// RPL_MYINFO - 004
    RplMyInfo {client: String, servername: String, version: String, avail_usermodes: String, avail_channelmodes: String, chanmodes_withparam: Option<String>}, 
    /// RPL_ISUPPORT - 005
    RplISupport {client_nick: String, feat_tokens: Vec<String>},
    // Auth
    Pass {password: String},
    Nick {nick: String},
    User {username: String, realname: String},
    Oper {username: String, password: String},
    // Channel operations
    Join {channels: Vec<String>, tokens: Option<Vec<String>>},
    Part {channels: Vec<String>, reason: Option<String>},
    Topic {channel: String, topic: Option<String>},
    Names {channels: Vec<String>},

    // Connection related
    Quit {reason: Option<String>},
    Error {reason: String},
    Ping {token: String},
    Pong {token: String},

    // Server queries
    Motd {target: String},
    Version {target: String},
    Admin {target: String},
    Connect {target: String, remote_addr: String, remote_port: u16},
    Time {target: String},
    Help {subject: String},
    Info,
    Mode {target: String, modestring: Option<String>, modeargs: Option<Vec<String>>},

    // Sending messages
    Privmsg {targets: Vec<String>, text: String},
    Notice {targets: Vec<String>, text: String}
}

impl TryFrom<RawIRCMessage> for IRCMessage {
    fn try_from(value: RawIRCMessage) -> Result<Self, <Self as TryFrom<RawIRCMessage>>::Error> {
        match value.command.as_str() {
            "001" => { // RplWelcome
                if value.parameters.len() >= 2 {
                    Ok(IRCMessage::RplWelcome { client_nick: value.parameters[0].clone(), msg: value.parameters[1].clone() })
                } else {
                    Err(anyhow!("Not enough parameters"))
                }
            }
            "002" => { // RplYourhost
                if value.parameters.len() >= 2 {
                    Ok(IRCMessage::RplYourhost { client_nick: value.parameters[0].clone(), msg: value.parameters[1].clone() })
                } else {
                    Err(anyhow!("Not enough parameters"))
                }
            }
            "003" => { // RplCreated
                if value.parameters.len() >= 2 {
                    Ok(IRCMessage::RplCreated { client_nick: value.parameters[0].clone(), msg: value.parameters[1].clone() })
                } else {
                    Err(anyhow!("Not enough parameters"))
                }
            }
            "004" => { // RplMyinfo
                if value.parameters.len() >= 5 {
                    Ok(IRCMessage::RplMyInfo { 
                        client: value.parameters[0].clone(), 
                        servername: value.parameters[1].clone(),
                        version: value.parameters[2].clone(),
                        avail_usermodes: value.parameters[3].clone(),
                        avail_channelmodes: value.parameters[4].clone(),
                        chanmodes_withparam: None
                     })
                } else {
                    Err(anyhow!("Not enough parameters"))
                }
            }
            "005" => { // RplWelcome
                if value.parameters.len() >= 2 {
                    Ok(IRCMessage::RplWelcome { client_nick: value.parameters[0].clone(), msg: value.parameters[1].clone() })
                } else {
                    Err(anyhow!("Not enough parameters"))
                }
            }
            _ => {
                todo!()
            }
        }
    }

    type Error = anyhow::Error;
}

#[derive(Debug)]
struct RawIRCMessage {
    tags: HashMap<String, String>,
    source: String,
    command: String,
    parameters: Vec<String>
}

impl Default for RawIRCMessage {
    fn default() -> RawIRCMessage {
        RawIRCMessage {
            tags: HashMap::default(),
            source: String::new(),
            command: String::new(),
            parameters: Vec::new()
        }
    }
}

impl RawIRCMessage {
    fn parse_from_string(msg: String) -> RawIRCMessage {
        let mut rawmsg = RawIRCMessage::default();
        let mut mode = 0;
        let mut current_tag_name = "".to_string();
        let mut current_tag_value = "".to_string();
        let mut curr_param = "".to_string();
        let mut new_param_lock = false;
        for char in msg.chars() {
            match mode {
                0 => {
                    match char {
                        '@' =>  {
                            mode = 1
                        }
                        ':' => {
                            mode = 3
                        }
                        _ => {
                            rawmsg.command += &char.to_string();
                            mode = 4
                        }
                    }
                }
                1 => { // Tags
                    match char {
                        ' ' => {
                            rawmsg.tags.insert(current_tag_name, current_tag_value);
                            // This is unnecessary, but I'll do it none the less
                            current_tag_name = "".to_string();
                            current_tag_value = "".to_string();
                            mode = 0
                        }
                        ';' => {
                            rawmsg.tags.insert(current_tag_name, current_tag_value);
                            current_tag_name = "".to_string();
                            current_tag_value = "".to_string();
                        }
                        '=' => {
                            mode = 2
                        }
                        _ => {
                            current_tag_name += &char.to_string();
                        }
                    }    
                }
                2 => { // Tag values
                    match char {
                        ' ' => {
                            rawmsg.tags.insert(current_tag_name, current_tag_value);

                            // This is unnecessary, but I'll do it none the less
                            current_tag_name = "".to_string();
                            current_tag_value = "".to_string();
                            mode = 0
                        }
                        ';' => {
                            rawmsg.tags.insert(current_tag_name, current_tag_value);
                            current_tag_name = "".to_string();
                            current_tag_value = "".to_string();
                            mode = 1
                        }
                        _ => {
                            current_tag_value += &char.to_string();
                        }
                    }    
                }
                3 => { // Source
                    match char {
                        ' ' => {
                            mode = 0
                        }
                        _ => {
                            rawmsg.source += &char.to_string();
                        }
                    }    
                }
                4 => { // Command
                    match char {
                        ' ' => {
                            mode = 5
                        }
                        _ => {
                            rawmsg.command += &char.to_string();
                        }
                    }                    
                }
                5 => { // Parameters
                    match char {
                        ' ' if !new_param_lock => {
                            rawmsg.parameters.push(curr_param);
                            curr_param = "".to_string()
                        }
                        ':' if !new_param_lock => {
                            new_param_lock = true;
                        }
                        _ => {
                            curr_param += &char.to_string();
                        }
                    }
                }
                _ => {
                    panic!("Unknown mode {} in RawIRCMessage parser; this is a bug!!!!", mode)
                }
            }
        }
        if curr_param.len() > 0 { // Incredibly hacky fix
            rawmsg.parameters.push(curr_param);
        }
        rawmsg.command.make_ascii_lowercase();
        rawmsg
    }
}

impl FromStr for RawIRCMessage {
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(RawIRCMessage::parse_from_string(s.to_string()))
    }
    
    type Err = anyhow::Error;
}

struct ConnectionState {
    nick: Option<String>,
    username: Option<String>,
    realname: Option<String>,
    channels_joined: Vec<String>,
    handshake_stage: u8,
}

impl Default for ConnectionState {
    fn default() -> ConnectionState {
        ConnectionState {
            nick: None,
            username: None,
            realname: None,
            channels_joined: Vec::new(),
            handshake_stage: 0
        }
    }
}

async fn sock_task(mut socket: TcpStream, globalchan: broadcast::Sender<GlobalMessage>) {
    let mut state = ConnectionState::default();
    
    let remote_addr = socket.peer_addr().map_or("<unknown>".to_string(), |x| x.to_string());
    let mut linereader = BufReader::new(socket).lines();
    log::info!("Connection opened by {remote_addr}");
    let mut gchan_rx = globalchan.subscribe();
    select! {
        possible_msg = gchan_rx.recv() => {
            if let Ok(msg) = possible_msg {
                //socket.write_all()
            } else {
                log::warn!("Failed to read global message; this is usually a bug!")
            }
        }
        possible_line = linereader.next_line() => {
            if let Ok(Some(line)) = possible_line && let Ok(msg) = line.parse::<RawIRCMessage>() {
                
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RawIRCMessage;

    #[test]
    fn test_rawircmessage_parse() {
        let msg: RawIRCMessage = "@a=;b=abcdef;c= :dan!d@localhost PRIVMSG #chan :hey :3".parse().expect("Failed to parse raw IRC message");
        assert_eq!(msg.command, "PRIVMSG");
        assert_eq!(msg.tags["a"], "");
        assert_eq!(msg.tags["b"], "abcdef");
        assert_eq!(msg.tags["c"], "");
        assert_eq!(msg.source, "dan!d@localhost");
        assert_eq!(msg.parameters[0], "#chan");
        assert_eq!(msg.parameters[1], "hey :3");
    }
}

#[tokio::main]
async fn main() {
    let (globalchan, _) = broadcast::channel::<GlobalMessage>(16);
    let listener = TcpListener::bind("127.0.0.1:6667").await.expect("Failed to bind socket; are 2 instances running at once?");

    loop {
        if let Ok((socket, _)) = listener.accept().await {
            tokio::task::spawn(sock_task(socket, globalchan.clone()));
        }   
    }
}
