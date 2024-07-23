const fs = require('fs');
const readline = require('readline');

const sourceFilename = 'demand-cache.csv';
const recordsPerFile = 100;

async function splitCSV() {
  const fileStream = fs.createReadStream(sourceFilename);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let fileIndex = 1;
  let recordCount = 0;
  let headerLine = '';
  let isFirstLine = true;
  let outputFileStream = null;

  for await (const line of rl) {
    if (isFirstLine) {
      headerLine = line; // Save the header line
      isFirstLine = false;

      // Create the first output file and write the header line
      outputFileStream = fs.createWriteStream(`output_${fileIndex}.csv`);
      outputFileStream.write(headerLine + '\n');
      continue;
    }

    if (recordCount === recordsPerFile) {
      outputFileStream.end();
      fileIndex++;
      recordCount = 0;

      // Create a new output file and write the header line
      outputFileStream = fs.createWriteStream(`output_${fileIndex}.csv`);
      outputFileStream.write(headerLine + '\n');
    }

    outputFileStream.write(line + '\n');
    recordCount++;
  }

  if (recordCount > 0) {
    outputFileStream.end();
  }

  console.log('CSV split completed');
}

splitCSV().catch((err) => console.error('Error during CSV split:', err));
