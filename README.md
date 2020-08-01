# decoder_agent

This decoder agent is to be used in conjunction with a decoder.

The agent's responsibilities are:
* Retrieving tasks from external Task Controller through http
* Downloading audio files to local storage for decoder to decode
* Receive decoding status updates from decoder and passes onward to Task Controller through http
* Uploading of transcriptions files to either AWS S3 or Azure Blob Storage

Environment variables that are needed can be found in env.example file.

`npm install` to install needed dependencies to run locally.

Build into docker image with Dockerfile to run in container.
