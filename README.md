# decoder_agent

This decoder agent is to be used in conjunction with a decoder.

The agent's responsibilities are:
* Retrieving tasks from external Task Controller through http
* Downloading audio files to local storage for decoder to detect and begin decoding
* Receive decoding status updates from decoder and passes onward to Task Controller through http
* Uploading of transcriptions files to either AWS S3 or Azure Blob Storage

Expected functionality of Decoder:
* Send status updates to agent via POST `/status`
    * Media type `application/json`
    * `status` field required
    * `status: "DONE"` will signal to the agent that decoding process is complete
    * Agent will forward other status updates to TaskController
* Send error update to agent via POST `/error`
    * Media type `application/json`
    * `status` field required
    * Agent will forward error status to TaskController
    * This signals to agent that decoder stopped decoding current file and is waiting for next file to decode

Environment variables that are needed can be found in env.example file.

`npm install` to install needed dependencies to run locally.

`node agent.js` to run agent.

Build into docker image with Dockerfile to run in container.
