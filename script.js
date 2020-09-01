const fs = require('fs');
const lineByLine = require('n-readlines');
const createCsvWriter = require('csv-writer');
const readline = require('readline');
const { google } = require('googleapis');

// If modifying these scopes, delete token.json.
const SCOPES = ['https://www.googleapis.com/auth/drive'];
// The file token.json stores the user's access and refresh tokens, and is
// created automatically when the authorization flow completes for the first
// time.
const TOKEN_PATH = 'token.json';

/**
 * Create an OAuth2 client with the given credentials, and then execute the
 * given callback function.
 * @param {Object} credentials The authorization client credentials.
 * @param {function} callback The callback to call with the authorized client.
 */
function authorize(credentials, callback) {
  const { client_secret, client_id, redirect_uris } = credentials.installed;
  const oAuth2Client = new google.auth.OAuth2(
    client_id, client_secret, redirect_uris[0]);

  // Check if we have previously stored a token.
  fs.readFile(TOKEN_PATH, (err, token) => {
    if (err) return getAccessToken(oAuth2Client, callback);
    oAuth2Client.setCredentials(JSON.parse(token));
    callback(oAuth2Client);
  });
}

/**
 * Get and store new token after prompting for user authorization, and then
 * execute the given callback with the authorized OAuth2 client.
 * @param {google.auth.OAuth2} oAuth2Client The OAuth2 client to get token for.
 * @param {getEventsCallback} callback The callback for the authorized client.
 */
function getAccessToken(oAuth2Client, callback) {
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
  });
  console.log('Authorize this app by visiting this url:', authUrl);
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  rl.question('Enter the code from that page here: ', (code) => {
    rl.close();
    oAuth2Client.getToken(code, (err, token) => {
      if (err) return console.error('Error retrieving access token', err);
      oAuth2Client.setCredentials(token);
      // Store the token to disk for later program executions
      fs.writeFile(TOKEN_PATH, JSON.stringify(token), (err) => {
        if (err) return console.error(err);
        console.log('Token stored to', TOKEN_PATH);
      });
      callback(oAuth2Client);
    });
  });
}

// Load client secrets from a local file.
fs.readFile('credentials.json', (err, content) => {
  if (err) return console.log('Error loading client secret file:', err);
  // Authorize a client with credentials, then call the Google Drive API.
  authorize(JSON.parse(content), createFolder);
});

const optionDefinitions = [
  { name: 'number', alias: 'n', type: Number, defaultValue: 10 },
  { name: 'await', alias: 'a', type: Number, defaultValue: 0 }, // set del tempo di attesa
  { name: 'image', alias: 'i', type: Boolean, defaultValue: false },
  { name: 'timeout', alias: 't', type: Number, defaultValue: 200000 } // tempo di attesa del caricamento tra una richiesta e l'altra
];
const commandLineArgs = require('command-line-args');
const { auth } = require('googleapis/build/src/apis/abusiveexperiencereport');
const options = commandLineArgs(optionDefinitions);

// Constant Values
const imagePath = 'inputDatasets/image.jpg';

const timeoutValue = options.timeout;
const numberOfBuses = options.number;
const awaitFor = options.await;
const image = options.image;
const inputBuses = 'inputDatasets/inputDataset' + numberOfBuses + '.csv';
const dirImg = image ? 'datasetIPFSImage/' : 'datasetIPFS/';
const dirTemp = dirImg + numberOfBuses + '/';
const dirService = dirTemp + 'dataService/';
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

const publish = async (b, id, json, prop, auth) => {
  const drive = google.drive({ version: 'v3', auth });
  let startTS = -1, finishTS = -1;
  // data = new Blob([JSON.stringify(json)], {type: 'application/json'}); provare con dump
  data = JSON.stringify(json);

  try {
    //Start operations
    startTS = new Date().getTime();
    if (image) {
      var fileName = 's' + startTS + '.jpg';
      var fileMetadata = {
        'name': fileName,
        parents: [global.folderId]
      };
      var media = {
        mimeType: 'image/jpeg',
        body: fs.createReadStream(imagePath)
      };
    } else {
      var fileName = 's' + startTS + '.csv';
      var fileMetadata = {
        'name': fileName,
        parents: [global.folderId]
      };
      var media = {
        mimeType: 'text/csv',
        body: data
      };
    }
    //first
    new Promise(async (resolve, reject) => {
      drive.files.create({
        resource: fileMetadata,
        media: media,
        fields: 'id'
      }, function (err, res) {
        if (err) {
          // Handle error
          console.log('Error uploading do gdrive: ' + err);
        } else { // if succeded
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
        }
      });
      resolve(true);
    })
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
const go = async (auth) => {
  const liner = new lineByLine(inputBuses);
  console.log('INPUT BUSES: ' + inputBuses)
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
          1,
          auth
        );
      
    }
  } catch (error) {
    console.log(error);
  }
};

async function createFolder(auth) {
  // creation of remote folder in gdrive
  const drive = google.drive({ version: 'v3', auth });
  // create folder in gdrive
  var fileMetadata = {
    'name': 'TestGDriveAPIs',
    'mimeType': 'application/vnd.google-apps.folder'
  };
  drive.files.create({
    resource: fileMetadata,
    fields: 'id'
  }, function (err, file) {
    if (err) {
      // Handle error
      console.error(err);
    } else {
      console.log('Folder Id: ', file.data.id);
      global.folderId = file.data.id
      main(auth)
    }
  });
}

const main = async (auth) => {
  console.log('FOLDER ID: ' + global.folderId);
  // minuti da aspettare prima di caricare file
  await sleep(awaitFor * 1000);
  setupEnvironment();
  await go(auth);
  console.log('Finished approximately at : ' + new Date().toString());
};

