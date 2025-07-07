use crate::commands::RedisResponse;
use crate::rdb_handler::RdbHandler;
use crate::server::{ReplicationState, ServerRole};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;

pub struct ReplicationMaster {
    _replication_state: ReplicationState,
    // Future: connected_replicas: Vec<ReplicaConnection>
}

impl ReplicationMaster {
    pub fn new(replication_state: ReplicationState) -> Self {
        Self {
            _replication_state: replication_state,
        }
    }

    pub async fn _handle_psync_request(&self, writer: &mut OwnedWriteHalf) -> Result<(), String> {
        // This method orchestrates the full resync process
        self._send_fullresync_response(writer).await?;
        self._send_rdb_file(writer).await?;
        Ok(())
    }

    async fn _send_fullresync_response(&self, writer: &mut OwnedWriteHalf) -> Result<(), String> {
        if let ServerRole::Master(master_state) = &self._replication_state.role {
            let response = RedisResponse::FullResync {
                repl_id: master_state.replid.clone(),
                offset: master_state.repl_offset,
            };

            writer
                .write_all(response.to_bytes().as_slice())
                .await
                .map_err(|e| format!("Failed to send FULLRESYNC: {}", e))?;
            writer
                .flush()
                .await
                .map_err(|e| format!("Failed to flush FULLRESYNC: {}", e))?;

            Ok(())
        } else {
            Err("Cannot send FULLRESYNC from non-master".to_string())
        }
    }

    pub async fn _send_rdb_file(&self, writer: &mut OwnedWriteHalf) -> Result<(), String> {
        // Send the RDB file after FULLRESYNC
        let rdb_data = RdbHandler::get_empty_rdb_file();
        let formatted_rdb = RdbHandler::format_rdb_transfer(&rdb_data);

        writer
            .write_all(&formatted_rdb)
            .await
            .map_err(|e| format!("Failed to send RDB file: {}", e))?;
        writer
            .flush()
            .await
            .map_err(|e| format!("Failed to flush RDB file: {}", e))?;

        Ok(())
    }
}
