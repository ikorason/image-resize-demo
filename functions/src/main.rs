// functions/src/main.rs

use azure_messaging_servicebus::service_bus::QueueClient;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::BlobServiceClient;
use serde::{Deserialize, Serialize};
use futures::StreamExt;
use tracing::trace;
use std::{env, io::Cursor};

#[derive(Serialize, Deserialize, Debug)]
struct ImageNode {
    filename: String,
    image_container: String,
}

#[tokio::main]
async fn main() -> azure_core::Result<()> {
    let service_bus_namespace = env::var("AZURE_SERVICE_BUS_NAMESPACE").expect("Please set AZURE_SERVICE_BUS_NAMESPACE env variable first!");
    let queue_name = env::var("AZURE_QUEUE_NAME").expect("Please set AZURE_QUEUE_NAME env variable first!");
    let policy_name = env::var("AZURE_POLICY_NAME").expect("Please set AZURE_POLICY_NAME env variable first!");
    let policy_key = env::var("AZURE_POLICY_KEY").expect("Please set AZURE_POLICY_KEY env variable first!");
    
    let http_client = azure_core::new_http_client();

    let client = QueueClient::new(
        http_client, 
        service_bus_namespace, 
        queue_name, 
        policy_name, 
        policy_key
    ).expect("Failed to create client");

    let received_message = client
        .receive_and_delete_message()
        .await
        .expect("Failed to receive message");

    if received_message.is_empty() {
        println!("No message received");
        return Ok(())
    }

    println!("Received message: {:?}", received_message);

    // grab the image from the message
    match serde_json::from_str::<ImageNode>(&received_message) {
        Ok(image) => {
            println!("Deserialized image: {:?}", image);

            // Azure Blob Storage credentials
            let storage_account = env::var("AZURE_STORAGE_ACCOUNT").expect("Missing AZURE_STORAGE_ACCOUNT env var");
            let storage_access_key = env::var("AZURE_STORAGE_ACCESS_KEY").expect("Missing AZURE_STORAGE_ACCESS_KEY env var");
            let container_name = image.image_container;

            let blob_name = &*image.filename; 

            // create Azure Blob Storage client
            let storage_credentials = StorageCredentials::access_key(storage_account.clone(), storage_access_key);
            let service_client = BlobServiceClient::new(storage_account, storage_credentials);
            let blob_client = service_client
                .container_client(&container_name)
                .blob_client(blob_name);

            trace!("Requesting blob");

            let mut bytes: Vec<u8> = Vec::new();
            // stream a blob, 8KB at a time
            let mut stream = blob_client.get().chunk_size(0x2000u64).into_stream();
            while let Some(value) = stream.next().await {
                let data = value?.data.collect().await?;
                println!("received {:?} bytes", data.len());
                bytes.extend(&data);
            }

            // load the image from the bytes
            let img = image::load_from_memory(&bytes).expect("Failed to load image");
            // resize the image
            let resized_img = img.resize(100, 100, image::imageops::FilterType::Triangle);
            // write the resized image to the buffer
            let mut resized_bytes: Vec<u8> = Vec::new();
            resized_img.write_to(&mut Cursor::new(&mut resized_bytes), image::ImageFormat::Jpeg).expect("Failed to write image");

            // change the filename to include the word "resized"
            let new_blob_name = format!("resized_{}", blob_name);

            let blob_client = service_client
                .container_client(&container_name)
                .blob_client(&new_blob_name);

            blob_client.put_block_blob(resized_bytes)
                .content_type("image/jpeg")
                .await
                .expect("Failed to upload blob");

            println!("Resized image uploaded successfully");

        },
        Err(e) => {
            println!("Failed to deserialize image: {:?}", e);
            return Ok(())
        }
    };

    Ok(())
}