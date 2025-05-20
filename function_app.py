import azure.functions as func
import json
import logging
import os
import uuid
from azure.storage.blob import BlobServiceClient
import azure.cognitiveservices.speech as speechsdk
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import tempfile
import threading

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
    filename = os.path.basename(myblob.name).replace(" ", "_")
    temp_dir = tempfile.gettempdir()
    temp_audio_path = os.path.join(temp_dir, filename)

    with open(temp_audio_path, "wb") as audio_file:
        audio_file.write(myblob.read())

    # Transcribe audio using Azure Speech-to-Text
    speech_key = os.getenv("AZURE_SPEECH_KEY")
    speech_region = os.getenv("AZURE_SPEECH_REGION")
    speech_config = speechsdk.SpeechConfig(subscription=speech_key, region=speech_region)
    
    # Enable speaker diarization
    speech_config.set_service_property(
        name="diarizationEnabled",
        value="true",
        channel=speechsdk.ServicePropertyChannel.UriQueryParameter
    )
    speech_config.set_service_property(
        name="diarizationSpeakerCount",
        value="2",
        channel=speechsdk.ServicePropertyChannel.UriQueryParameter
    )

    # Set up audio and transcriber
    audio_config = speechsdk.AudioConfig(filename=temp_audio_path)
    transcriber = speechsdk.transcription.ConversationTranscriber(
        speech_config=speech_config, audio_config=audio_config)

    transcript_lines = []
    done = threading.Event()

    def handle_transcribed(evt):
        speaker = evt.result.speaker_id
        label = f"Speaker {speaker}" if speaker else "Unknown"
        if evt.result.text:
            transcript_lines.append(f"{label}: {evt.result.text}")
            logging.info(f"{label}: {evt.result.text}")

    def handle_canceled(evt):
        logging.warning(f"Transcription canceled: {evt.reason}")
        if evt.reason == speechsdk.CancellationReason.Error:
            logging.error(f"Error details: {evt.error_details}")
        done.set()

    def handle_session_stopped(evt):
        logging.info("Transcription session stopped.")
        done.set()

    transcriber.transcribed.connect(handle_transcribed)
    transcriber.canceled.connect(handle_canceled)
    transcriber.session_stopped.connect(handle_session_stopped)

    # Start transcription and wait until complete
    transcriber.start_transcribing_async().get()
    done.wait()
    transcriber.stop_transcribing_async().get()

    # Compile final transcript
    transcript = "\n".join(transcript_lines)

    # Generate a random call_id
    call_id = str(uuid.uuid4())

    # Prepare transcript as JSON with call_id
    transcript_json = json.dumps({
        "call_id": call_id,
        "transcript": transcript
    })

    # Send transcript to Service Bus Topic
    servicebus_conn_str = os.getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
    topic_name = os.getenv("AZURE_SERVICEBUS_TOPIC_NAME", "transcripttopic")
    with ServiceBusClient.from_connection_string(servicebus_conn_str) as client:
        sender = client.get_topic_sender(topic_name=topic_name)
        with sender:
            message = ServiceBusMessage(transcript_json)
            sender.send_messages(message)
    
    logging.info("Transcript sent to Service Bus topic.")