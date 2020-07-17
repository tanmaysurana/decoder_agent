const express = require('express')
const axios = require('axios')
const disk = require('diskusage')
const os = require('os')
const fs = require('fs')
const path = require('path')
const AdmZip = require('adm-zip')

const AWS = require("aws-sdk")
const {BlobServiceClient, StorageSharedKeyCredential} = require("@azure/storage-blob")

const dotenv = require('dotenv')
dotenv.config()

////////////////////////////////////////////////////////////////////////////////
// global variables
////////////////////////////////////////////////////////////////////////////////
const aws_access_key_id       = process.env.AWS_ACCESS_KEY_ID
const aws_secret_access_key   = process.env.AWS_SECRET_ACCESS_KEY
const aws_bucket              = process.env.AWS_BUCKET  // transcription files get uploaded to this bucket

const azure_account           = process.env.AZURE_ACCOUNT
const azure_account_key       = process.env.AZURE_ACCOUNT_KEY
const azure_container         = process.env.AZURE_CONTAINER

const use_storage             = process.env.USE_STORAGE  // aws | azure (azure currently not supported)

const taskControllerEndpoint  = process.env.TASKCONTROLLER_URL
const intervalPeriod          = 10000 // milliseconds
const port                    = process.env.PORT
const root = os.platform()  === 'win32' ? 'c:' : '/'
const num_bytes_for_buffer    = 100000  // 100kb, buffer space for transcription
const worker_queue            = process.env.WORKER_QUEUE  // default value is 'normal'
const worker_name             =`${os.hostname}-${process.pid}`
const worker_language         = process.env.WORKER_LANGUAGE // english | mandarin | malay

var original_filename         = undefined  // filename received from taskcontroller
var converted_filename        = undefined  // filename after decoder renames (if renaming is enabled)
var isProcessing              = false      // flag to see if currently decoding
var processingInterval        = undefined  // interval to get pending task
var task_id                   = undefined  // task_id of currently processing task
var errorTimeout              = undefined  // timeout if decoder takes too long
var putFileInCloud            = undefined  // for switching cloud storage

// AZURE currently not supported
if (use_storage === 'aws') {
  putFileInCloud = putFileIntoS3
}
else if (use_storage === 'azure') {
  putFileInCloud = putFileIntoAzure
}

////////////////////////////////////////////////////////////////////////////////
// end of global variables
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// AWS S3 related
////////////////////////////////////////////////////////////////////////////////
AWS.config = new AWS.Config({
  // not needed because it looks at ENV variable already
  // accessKeyId: aws_access_key_id,
  // secretAccessKey: aws_secret_access_key
})

var s3 = (use_storage === 'aws') ? new AWS.S3() : undefined

function putFileIntoS3(key, data) {
  return new Promise( (resolve, reject) => {
    var params = {
      Bucket: aws_bucket,
      Key: key,
      Body: data,
      ACL: "public-read"
      // Metadata: ""
    }
    s3.upload(params, (err, data) => {
      // data.Location = URL of uploaded object
      // data.ETag
      // data.Bucket
      // data.Key
      console.log(`${key} uploaded to AWS bucket ${aws_bucket}`)
      resolve(data.Location)
    })
  })
}
////////////////////////////////////////////////////////////////////////////////
// end of AWS S3 related
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// Azure Storage
////////////////////////////////////////////////////////////////////////////////
var containerClient = undefined

if (use_storage === 'azure')  {
  const sharedKeyCredential = new StorageSharedKeyCredential(azure_account, azure_account_key)
  const blobServiceClient = new BlobServiceClient(
    `https://${azure_account}.blob.core.windows.net`,
    sharedKeyCredential
  )
  containerClient = blobServiceClient.getContainerClient(azure_container)
}


async function putFileIntoAzure(key, data) {
  return new Promise( (resolve, reject) => {

    const blockBlobClient = containerClient.getBlockBlobClient(key)

    var objectURL = path.join(containerClient.url, encodeURIComponent(key))

    const uploadBlobResponse = blockBlobClient.upload(data, Buffer.byteLength(data))
    uploadBlobResponse.then(val => {
      console.log(`${key} uploaded to Azure storage container ${containerClient.containerName}`)
      resolve(objectURL)
    }).catch(error => {
      console.log(`Error during uploading to Azure: ${error}`)
    })
  })
}
////////////////////////////////////////////////////////////////////////////////
// end of Azure Storage
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// task controller communication functions declarations
////////////////////////////////////////////////////////////////////////////////
function getPendingTask(options) {

  console.log(`${new Date()} Checking for pending task...`)
  // make connection to TaskController and check for pending task
  // if pending task available, proceed to download from S3
  var params = {
    type: "reserve",
    worker: worker_name,
    lang: worker_language,
    queue: worker_queue,
  }

  axios.post(`${taskControllerEndpoint}/tasks/actions`, params)
  .then(response => {
    // response should be a json object according to the specified format
    // get file from S3 based on response

    // if response === null, no pending task
    if (response.data.task) {
      var task = response.data.task
      console.log("Task received.")
      console.log(response.data)
      if (task.queue === worker_queue) {

        // eg. endpoint = "test-bucket-123abc-test.s3-ap-southeast-1.amazonaws.com/recording.wav"
        // eg. Bucket   = "test-bucket-123abc-test"
        // eg. Key      = "recording.wav"

        original_filename = task.data['filename']
        converted_filename = original_filename
        task_id = task.task_id
        var cloud_url = task.data["cloud-link"]

        isProcessing = true

        getFileFromURL(cloud_url, original_filename).then( val => {
          storeAudioFile(val)
        })
      }
    }
  })
  .catch(error => {
    console.log(`Error checking for pending task: ${error}`)
    sendFailureStatus("TASK_FIELDS_ERROR")
  })
}

function sendStatus(status) {

  var params = {
    type: "progress",
    worker: worker_name,
    progress: status
  }

  axios.post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
  .then(response => {
    console.log(`Status ${status} sent to TaskController.`)
  })
  .catch(error => {
    console.log(`Error sending ${status} status to TaskController: ${error}`)
    console.log(error.response.data)
  })
}

function sendSuccessStatus(cloud_link) {

  isProcessing = false
  clearTimeout(errorTimeout)

  var params = {
    type: "success",
    worker: worker_name,
    result: {
      "cloud-link": cloud_link
    }
  }

  axios.post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
  .then(response => {
    task_id = undefined
    console.log(`Success status sent to TaskController.`)
  })
  .catch(error => {
    console.log(`Error sending success status to TaskController: ${error}`)
  })
}

function sendFailureStatus(error_message) {

  isProcessing = false
  clearTimeout(errorTimeout)

  var params = {
    type: "error",
    worker: worker_name,
    err_code: error_message,
  }

  axios.post(`${taskControllerEndpoint}/tasks/${task_id}/actions`, params)
  .then(response => {
    task_id = undefined
    console.log(`Failure status sent to TaskController.`)
  })
  .catch(error => {
    console.log(`Error sending failure status ${error_message} to TaskController: ${error}`)
  })
}
////////////////////////////////////////////////////////////////////////////////
// end of task controller communication functions declarations
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// internal function declarations
////////////////////////////////////////////////////////////////////////////////
function initialize() {
  // check for pending task
  processingInterval = setInterval(()=>{
    if (!isProcessing) getPendingTask()
  }, intervalPeriod)
}

function getFileFromURL(url, dest) {
  return new Promise((resolve, reject) => {
    axios.get(url)
    .then(response => {
      var content_length = Number(response.headers["content-length"])
      var data = response.data
      var val = {
        filename: dest,
        contentLength: content_length,
        data: data,
      }
      resolve(val)
    })
    .catch(error => {
      console.log(error)
      sendFailureStatus("DOWNLOAD_ERROR")
    })
  })
}

async function storeAudioFile(val) {
  // need to check if enough storage space before writing to disk
  let avail = await getAvailableSpace()

  var data = val.data
  var contentLength = val.contentLength
  var filename = val.filename

  errorTimeout = setTimeout(() => {
    sendFailureStatus("DECODING_TIMEOUT")
    cleanUpDecoderFiles()
  }, contentLength*1.5)

  if (contentLength + num_bytes_for_buffer > avail) {
    console.log("Not enough disk space.")
    // inform about failed decoding
    sendFailureStatus("LOCALDISK_FULL")
    cleanUpDecoderFiles()
  }
  else {
    fs.writeFile(`input/${filename}`, data, (err) => {
      if (err) {
        console.log(`Writing data to file unsuccessful: ${err}`)
        sendFailureStatus("SAVING_FILE_ERROR")
      }
      else{
        console.log(`Writing ${contentLength} bytes to file successful.`)
      }
    })
  }
}

function sendTranscriptionFiles(callback) {
  return new Promise( (resolve, reject) => {
    var name = path.parse(converted_filename).name
    var original_name = path.parse(original_filename).name
    fs.readdir(`./output/${name}`, (err, files) => {

      var zip = new AdmZip()
      files.forEach((itemname) => {
        zip.addLocalFile(`./output/${name}/${itemname}`)
      });

      var zipBuffer = zip.toBuffer()
      callback(`${original_name}.zip`, zipBuffer).then( val => {
        resolve(val)
      })
    })
  })
}

function cleanUpDecoderFiles() {
  // remove audio file
  fs.unlink(`./input/${converted_filename}`, (error) => {
    if (error) console.log(`Error during removal of input file: ${error}`)
    else console.log(`Input file ${converted_filename} removed.`)
  })

  var name = path.parse(converted_filename).name

  // remove details files
  fs.rmdir(`./details/${name}`, {recursive: true}, (error) => {
    if (error) console.log(`Error during removal of details files: ${error}`)
    else console.log(`Details files for ${name} removed.`)
  })

  // remove transcription files
  fs.rmdir(`./output/${name}`, {recursive: true}, (error) => {
    if (error) console.log(`Error during removal of output files: ${error}`)
    else console.log(`Output files for ${name} removed.`)
  })
}

async function getAvailableSpace() {
  return new Promise(async (resolve) => {
    var { available } = await disk.check(root)
    console.log(`Local Disk availble space: ${available} bytes`)
    resolve(available)
  })
}
////////////////////////////////////////////////////////////////////////////////
// end of internal function declarations
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// http server declaration
////////////////////////////////////////////////////////////////////////////////
const app = express()

// middleware json
app.use(express.json())

app.get('/', (req, res) => {
  res.send("Hello I am alive")
})

app.post('/status', (req, res) => {
  const body = req.body

  converted_filename = body.filename
  var status = body.status
  var channel = body.channel

  res.send("Status received")

  console.log(`DECODER: ${converted_filename} is ${status}.`)
  sendStatus(status)

  // upload transcription to AWS S3
  if (status === "DONE") {
    sendTranscriptionFiles(putFileInCloud).then( val => {
      sendSuccessStatus(val)
      cleanUpDecoderFiles()
    })
  }
})

app.post('/error', (req, res) => {
  const body = req.body

  res.send("Error received.")

  console.log(body)

  sendFailureStatus("DECODE_ERROR")
  cleanUpDecoderFiles()
})

app.get('/stop', (req, res) => {
  clearInterval(processingInterval)
  processingInterval = undefined

  function checkProcessing() {
    if (isProcessing) {
      setTimeout(checkProcessing, 1000)
    }
    else {
      res.send("Processing has finished and will not get new tasks.")
    }
  }
  checkProcessing()
})

app.listen(port, () => {
  console.log(`Listening on port ${port}`)
})
////////////////////////////////////////////////////////////////////////////////
// end of http server declaration
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
// execution starts here
////////////////////////////////////////////////////////////////////////////////
initialize()
