// api/src/main.rs

use azure_messaging_servicebus::service_bus::QueueClient;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::ClientBuilder;
use bytes::BufMut;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use warp::{
    http::StatusCode,
    multipart::{FormData, Part},
    Filter, Rejection, Reply,
};
use std::{convert::Infallible, env};

#[derive(Serialize, Deserialize, Debug)]
struct Image {
    filename: String,
    image_container: String,
}

#[tokio::main]
async fn main() {
    let upload_route = warp::path("upload")
        .and(warp::post())
        .and(warp::multipart::form().max_length(5 * 1024 * 1024)) // Max image size: 5MB
        .and_then(upload_file);

    let routes = upload_route
        .recover(handle_rejection);

    println!("Server started at http://localhost:3030");
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

async fn upload_file(form: FormData) -> Result<impl Reply, Rejection> {
    let uploaded_files: Vec<_> = form
        .and_then(|mut part: Part| async move {
            let mut bytes: Vec<u8> = Vec::new();

            // read the part stream
            while let Some(content) = part.data().await {
                let content = content.unwrap();
                bytes.put(content);
            }

            if !bytes.is_empty() {
                // Azure Blob Storage credentials
                let storage_account = env::var("AZURE_STORAGE_ACCOUNT").expect("Missing AZURE_STORAGE_ACCOUNT env var");
                let storage_access_key = env::var("AZURE_STORAGE_ACCESS_KEY").expect("Missing AZURE_STORAGE_ACCESS_KEY env var");
                let container_name = env::var("AZURE_STORAGE_CONTAINER").expect("Missing AZURE_STORAGE_CONTAINER env var");
                let blob_name = part.filename().unwrap().to_string(); 

                // create Azure Blob Storage client
                let storage_credentials = StorageCredentials::access_key(storage_account.clone(), storage_access_key);
                let blob_client = ClientBuilder::new(storage_account, storage_credentials).blob_client(&container_name, blob_name);

                // upload file to Azure Blob Storage
                match blob_client
                    .put_block_blob(bytes.clone())
                    .content_type("image/jpeg")
                    .await {
                        Ok(_) => println!("Blob uploaded successfully"),
                        Err(e) => println!("Error uploading blob: {:?}", e),
                    }

                println!("Uploaded file url: {}", blob_client.url().expect("Failed to get blob url"));

                let image = Image {
                    filename: part.filename().unwrap().to_string(),
                    image_container: container_name,
                };

                send_message_to_queue(image).await;
            }

            // return the part name, filename and bytes as a tuple
            Ok((
                part.name().to_string(),
                part.filename().unwrap().to_string(),
                String::from_utf8_lossy(&*bytes).to_string(),
            ))
        })
        .try_collect()
        .await
        .map_err(|_| warp::reject::reject())?;

    Ok(format!("Uploaded files: {:?}", uploaded_files))
}

async fn send_message_to_queue(image: Image) {
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

    let message_to_send = serde_json::to_string(&image).expect("Failed to serialize image");

    client
        .send_message(message_to_send.as_str())
        .await
        .expect("Failed to send message");

    println!("Message sent to Azure Service Bus queue successfully!");
    println!("Message: {}", message_to_send);
}

async fn handle_rejection(err: Rejection) -> std::result::Result<impl Reply, Infallible> {
    let (code, message) = if err.is_not_found() {
        (StatusCode::NOT_FOUND, "Not Found".to_string())
    } else if err.find::<warp::reject::PayloadTooLarge>().is_some() {
        (StatusCode::BAD_REQUEST, "Payload too large".to_string())
    } else {
        eprintln!("unhandled error: {:?}", err);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error".to_string(),
        )
    };

    Ok(warp::reply::with_status(message, code))
}