use std::error::Error;
use std::io::Cursor;
use bytes::{Buf, BytesMut};
use tokio::{io, join, select};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tart_backend::decoder::{decode_message_frame, Decode, DecodingError};
use tart_backend::{Event, NodeInformation};

pub struct CollectedInput {
    pub data: Vec<u8>,
    pub event_count: usize,
}

enum ProtocolState {
    NodeInformation,
    Events {
        core_count: u16,
    },
}

/// Creates a [`TcpListener`] bound to `client` address that forwards all traffic to `server`
/// address.
pub async fn collect_input(client: &str, server: &str) -> Result<CollectedInput, Box<dyn Error + Sync + Send>> {
    let listener = TcpListener::bind(client).await?;

    loop {
        let (client, _addr) = listener.accept().await?;
        let server = TcpStream::connect(server).await?;

        let (mut client_reader, mut client_writer) = client.into_split();
        let (mut server_reader, mut server_writer) = server.into_split();

        let client_to_server = tokio::spawn(async move {
            let mut writer_offset = 0;
            let mut event_count = 0;
            let mut data = Vec::new();
            let writer = &mut data;
            let mut buf = BytesMut::new();
            let buf_ref = &mut buf;
            let mut state = ProtocolState::NodeInformation;

            while {
                let n = client_reader.read_buf(buf_ref).await?;

                if n > 0 {
                    io::copy(&mut &buf_ref[buf_ref.len() - n..buf_ref.len()], writer).await?;
                }
                if let ProtocolState::NodeInformation = &state {
                    if let Some((node_info, length)) = extract_node_information(writer).await? {
                        writer_offset = length;

                        state = ProtocolState::Events { core_count: node_info.params.core_count };
                    }
                }
                if let ProtocolState::Events { core_count } = &state {
                    if let Some((event, length)) = extract_events(&writer[writer_offset..], *core_count).await? {
                        writer_offset += length;
                        event_count += 1;
                    }
                }

                n != 0
            } {
                server_writer.write_buf(buf_ref).await?;
            }

            while buf_ref.has_remaining() {
                server_writer.write_buf(buf_ref).await?;
            }

            Ok::<_, Box<dyn std::error::Error + Sync + Send>>(
                CollectedInput { data, event_count }
            )
        });
        let server_to_client = tokio::spawn(async move {
            io::copy(&mut server_reader, &mut client_writer).await
        });

        let (data, _) = join!(client_to_server, server_to_client);

        return data?;
    }
}

pub async fn extract_node_information<B>(
    buffer: &B,
) -> Result<Option<(NodeInformation, usize)>, DecodingError>
where
    B: AsRef<[u8]> + ?Sized,
{
    match decode_message_frame(buffer.as_ref()) {
        Ok((size, msg_data)) => {
            let mut cursor = Cursor::new(msg_data);

            match NodeInformation::decode(&mut cursor) {
                Ok(info) => Ok(Some((info, 4 + size as usize))),
                Err(e) => Err(e),
            }
        }
        Err(DecodingError::InsufficientData { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

pub async fn extract_events<B>(
    buffer: &B,
    core_count: u16,
) -> Result<Option<(Event, usize)>, DecodingError>
where
    B: AsRef<[u8]> + ?Sized,
{
    match decode_message_frame(buffer.as_ref()) {
        Ok((size, msg_data)) => {
            let mut cursor = Cursor::new(msg_data);

            match Event::decode_event(&mut cursor, core_count) {
                Ok(event) => Ok(Some((event, 4 + size as usize))),
                Err(e) => Err(e),
            }
        }
        Err(DecodingError::InsufficientData { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}
