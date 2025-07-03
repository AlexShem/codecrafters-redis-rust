use tokio::io::{BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

pub enum Command {
    Ping,
    Echo(String)
}

pub struct Server {
    pub reader: BufReader<OwnedReadHalf>,
    pub writer: BufWriter<OwnedWriteHalf>
}

impl Server {
    pub fn new(stream: TcpStream) -> Self {
        let (read, write) = stream.into_split();
        let reader = BufReader::new(read);
        let writer = BufWriter::new(write);
        Server {reader, writer}
    }

    pub fn parse_resp_array(&mut self, command_raw: String) -> anyhow::Result<Command> {
        let parts: Vec<String> = command_raw.split("\r\n").map(|s| s.to_string()).collect();
        let command_name = &parts[2];
        match command_name.to_uppercase().as_str() { 
            "PING" => {
                Ok(Command::Ping)
            } ,
            "ECHO" => {
                let arg = &parts[4];
                Ok(Command::Echo(arg.to_owned()))
            }
            _ => Err(anyhow::anyhow!("Unknown command"))
        }
    }
}