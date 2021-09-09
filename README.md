# decoder_agent

This decoder agent is to be used in conjunction with a decoder.

The agent's responsibilities are:
* Retrieving tasks from external Task Controller through http
* Downloading audio files to local storage for decoder to detect and begin decoding
    * Audio files will be downloaded to `input/` directory
    * For Docker/Kubernetes deployments, Agent and Decoder will both need to access this same directory
* Receive decoding status updates from decoder and passes onward to Task Controller through http
* Uploading of transcriptions files to local storage
    * Transcription files are assumed to be in `output/` directory
    * If audio file has the name `abcdef.wav`, the resulting output files are assumed to be in `output/abcdef/` directory
    * The entire `output/abcdef/` directory will be written to the destination local path
    * For Docker/Kubernetes Deployments, Agent and Decoder will both need to access this same directory
* For Docker images built with the Dockerfile, `input/` and `output/` directories' full paths are `/usr/src/app/input/` and `/usr/src/app/output/` respectively

Expected functionality of Decoder:
* Send status updates to agent via POST `/status`
    * Media type `application/json`
    * Request body should have these fields
        * `{ "status": string, "filename": string }`
        * If `filename` provided by Decoder is different from the original filename obtained from the TaskController, Agent will take the new `filename` value when searching for the output transcription files
    * `status: "DONE"` will signal to the Agent that decoding process is complete and will write the transcription files to the local destination path 
    * For all other statuses, Agent will simply forward them to TaskController
* Send error update to agent via POST `/error`
    * Media type `application/json`
    * Request body should have these fields
        * `{ "status": string }`
    * Agent will forward error status to TaskController and begin cleanup
    * This signals to the Agent that decoding process for the current file has stoppped and is waiting for a new file to begin deocding process again

Environment variables:
```
TRANSCRIPTION_DESTINATION=      # transcription file destination

PORT=                           # port number that the Agent will run HTTP server
TASKCONTROLLER_URL=             # URL of TaskController to receive tasks from
WORKER_QUEUE=                   # task's queue to filter for
WORKER_LANGUAGE=                # task's language to filter for

# OPTIONAL
DECODER_START_WAIT_TIME=        # time to wait for decoder to send "STARTING" status update, in milliseconds, defaults to '30000'
WORKER_SAMPLING_RATE=           # task's sampling rate to filter for, defaults to '16khz'
POLLING_PERIOD=                 # how often it polls TaskController for new tasks in milliseconds, defaults to '15000'
```

`npm install` to install needed dependencies to run locally.

`node src/agent.js` to run agent.

Build into docker image with Dockerfile to run in container.

