const fs = require('fs');
const lineByLine = require('n-readlines');
const createCsvWriter = require('csv-writer');
const readline = require('readline');
const Bottleneck = require('bottleneck');
const AWS = require('aws-sdk');

const limiter = new Bottleneck({ minTime: 110 });

// inserire credenziali 
const ID = '';
const SECRET = '';

const BUCKET_NAME = 'testingawstesi';

const s3 = new AWS.S3({
  accessKeyId: ID,
  secretAccessKey: SECRET
});

const optionDefinitions = [
  { name: 'number', alias: 'n', type: Number, defaultValue: 10 },
  { name: 'await', alias: 'a', type: Number, defaultValue: 0 }, // set del tempo di attesa
  { name: 'image', alias: 'i', type: Boolean, defaultValue: false },
  { name: 'timeout', alias: 't', type: Number, defaultValue: 200000 }, // tempo di attesa del caricamento tra una richiesta e l'altra
];
const commandLineArgs = require('command-line-args');
const options = commandLineArgs(optionDefinitions);

// Constant Values
const imagePath = 'inputDatasets/image.jpg';
const timeoutValue = options.timeout;
const numberOfBuses = options.number;
const awaitFor = options.await;
const image = options.image;
const inputBuses = 'inputDatasets/inputDataset' + numberOfBuses + '.csv';
const dirTemp = 'datasetIPFS/' + numberOfBuses + '/';
const dirImg = image ? 'image/' : 'dataService/';
const dirService = dirTemp + dirImg;
let dirDate;
let bus;

const sleep = (ms) => {
  return new Promise((resolve) => setTimeout(resolve, ms, false));
};

const setupEnvironment = () => {
  bus = {};

  if (!fs.existsSync(dirTemp)) fs.mkdirSync(dirTemp);
  if (!fs.existsSync(dirService)) fs.mkdirSync(dirService);
  dirDate = new Date().toISOString();
  const dirTime = dirService + dirDate;
  // creazione cartella con timestamp
  if (!fs.existsSync(dirTime)) fs.mkdirSync(dirTime);
};

const initBus = async (busID) => {
  try {
    // Bus object
    bus[busID] = {
      csv: '',
    };
    // Create log file
    const filepath = (bus[busID].csv =
      dirService + dirDate + '/bus-' + busID + '.csv');
    fs.writeFile(filepath, 'start,finish,counter\n', (err) => {
      if (err) throw err;
    });
  } catch (error) {
    console.log('SETUP ERROR: ' + error);
  }
};

const publish = async (b, id, json, prop) => {
  let startTS = -1,
    finishTS = -1;
  data = JSON.stringify(json, null, 2);

  try {
    //Start operations
    startTS = new Date().getTime();
    if (image) {
      var fileName = 's' + startTS + '.jpg';
      data = fs.createReadStream(imagePath);
    } else {
      var fileName = 's' + startTS + '.json';
      //fs.writeFileSync(fileName, data);
    }

    // Setting up S3 upload parameters
    const params = {
      Bucket: BUCKET_NAME,
      Key: fileName, // File name you want to save as in S3
      Body: data
  };
    //first
    limiter.schedule(
      () =>
      new Promise(async (resolve, reject) => {
        // Uploading files to the bucket
        s3.upload(params, function (err, data) {
          if (err) {
            console.log('Error uploading do AWS: ' + err);
            //!image ? fs.unlinkSync(fileName) : '';
            throw err;
          } else {
            // if succeded
            finishTS = new Date().getTime();
            // Latency measures
            r = finishTS - startTS;
            // Log result
            console.log(prop + ') bus ' + b + ': ' + r + 'ms');
            fs.appendFile(
              bus[b].csv,
              startTS + ',' + finishTS + ',' + id + '\n',
              (err) => {
                if (err) throw err;
              }
            );
            //!image ? fs.unlinkSync(fileName) : '';
            //console.log(`File uploaded successfully. ${data.Location}`);
          }
        });
        resolve(true);
      })
    );
    //second
    sleep(timeoutValue);
  } catch (err) {
    console.log(prop + ')' + b + ': ' + err);
    fs.appendFile(
      bus[b].csv,
      startTS + ',' + finishTS + ',' + id + '\n',
      (err) => {
        if (err) throw err;
      }
    );
  }
};

// Main phase, reading buses behavior in order to publish messages to MAM channels
const go = async () => {
  const liner = new lineByLine(inputBuses);
  try {
    let line = liner.next(); // read first line
    while ((line = liner.next())) {
      let row = line.toString('ascii').split(',');
      if (bus[row[1]] == undefined) initBus(row[1]);

      console.log('Waiting ' + row[0]);
      await sleep(parseInt(row[0]) * 1000);
      //console.log('Waited ' + row[0] + ' seconds for bus ' + row[1]);
      const payloadValue = { latitude: row[2], longitude: row[3] };

      publish(
        row[1],
        row[4],
        {
          payload: payloadValue,
          timestampISO: new Date().toISOString(),
        },
        1
      );
    }
  } catch (error) {
    console.log(error);
  }
};

async function createFolder() {
  // creation of remote folder in gdrive
  const drive = google.drive({ version: 'v3', auth });
  // create folder in gdrive
  var fileMetadata = {
    name: 'TestGDriveAPIs',
    mimeType: 'application/vnd.google-apps.folder',
  };
  await limiter.schedule(() =>
    drive.files.create(
      {
        resource: fileMetadata,
        fields: 'id',
      },
      function (err, file) {
        if (err) {
          // Handle error
          console.error(err);
        } else {
          console.log('Folder Id: ', file.data.id);
          global.folderId = file.data.id;
        }
      }
    )
  );
}

const main = async (auth) => {
  // minuti da aspettare prima di caricare file
  await sleep(awaitFor * 1000);
  setupEnvironment();
  await go(auth);
  console.log('Finished approximately at : ' + new Date().toString());
};

main();
