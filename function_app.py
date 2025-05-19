import azure.functions as func
import json
import logging
import os
from azure.storage.blob import BlobServiceClient
import azure.cognitiveservices.speech as speechsdk
from azure.servicebus import ServiceBusClient, ServiceBusMessage

app = func.FunctionApp()

@app.function_name(name="blobberfunction")
@app.blob_trigger(arg_name="myblob",
                  path="blobber-container/{name}",
                  connection="AzureWebJobsStorage")
def blobberfunction(myblob: func.InputStream):
    logging.info(f"Blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    # Download audio blob to temp file
    temp_audio_path = f"/tmp/{myblob.name}"
    with open(temp_audio_path, "wb") as audio_file:
        audio_file.write(myblob.read())

    # Transcribe audio using Azure Speech-to-Text
    speech_key = os.getenv("AZURE_SPEECH_KEY")
    speech_region = os.getenv("AZURE_SPEECH_REGION")
    speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=speech_region)
    audio_config = speechsdk.AudioConfig(filename=temp_audio_path)
    recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config)
    result = recognizer.recognize_once()
    transcript = result.text

    # Send transcript to Service Bus Topic
    servicebus_conn_str = os.getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
    topic_name = os.getenv("AZURE_SERVICEBUS_TOPIC_NAME", "transcripttopic")
    with ServiceBusClient.from_connection_string(servicebus_conn_str) as client:
        sender = client.get_topic_sender(topic_name=topic_name)
        with sender:
            message = ServiceBusMessage(transcript)
            sender.send_messages(message)
    
    logging.info("Transcript sent to Service Bus topic.")