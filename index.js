require("dotenv").config();
const fs = require("fs");
const util = require("util");
const axios = require("axios");
const moment = require("moment");
const uniqBy = require("lodash/uniqBy");
const argv = require("yargs")
  .usage("Usage: $0 [sym1] [sym2] [startDate]")
  .demandCommand(3).argv;
const readFileP = util.promisify(fs.readFile);
const writeFileP = util.promisify(fs.writeFile);

const unit = "hours";
let [sym1, sym2, startDate] = argv._;
const filename = `${sym1}_${sym2}_cc.json`;
const targetRangeStart = moment(startDate);
const targetRangeEnd = moment().startOf(unit);
const dateDisplayFormat = "YYYY-MM-DD HH:mm:ss";

const maxUnitsInBatch = 2000;

const ccApi = axios.create({
  baseURL: process.env.API_URL || "https://min-api.cryptocompare.com",
  headers: {
    authorization: `Apikey ${process.env.API_KEY}`
  }
});

async function main() {
  console.log(
    `--- Fetching cryptocompare data for ${sym1}-${sym2} from ${moment(
      targetRangeStart
    ).format(dateDisplayFormat)} until now ---`
  );
  console.log();

  let data = await loadFile();
  let [toDate, nUnits] = getNextParams({
    targetRangeStart,
    targetRangeEnd,
    data
  });

  while (nUnits > 0) {
    const newData = await fetchCCHourly(toDate, nUnits);
    data = mergeData(data, newData);
    await saveFile(data);
    [toDate, nUnits] = getNextParams({
      targetRangeStart,
      targetRangeEnd,
      data
    });
  }
}

function getNextParams({ targetRangeStart, targetRangeEnd, data }) {
  const [dataRangeStart, dataRangeEnd] = minMaxDates(data, targetRangeEnd);
  let toDate = null;
  let nUnits = 0;

  if (targetRangeEnd.isSame(dataRangeEnd)) {
    // only oldest entries are potentially missing
    /*
    NOW
    _   _ targetRangeEnd and dataRangeEnd
    |   |
    |   _ dataRangeStart
    |   chunk1
    |     chunk2
    |       chunk3
    _ targetRangeStart
    PAST
    */
    toDate = dataRangeStart;
    nUnits = countUnits(targetRangeStart, dataRangeStart);
  } else {
    // we're missing some data on top (newest entries)
    /*
    NOW
    _ targetRangeEnd
    |       chunk3
    |     chunk2
    |   chunk1
    |   _ dataRangeEnd
    |   |
    |   |
    ... ...
    PAST
    */
    nUnits = countUnits(dataRangeEnd, targetRangeEnd);
    toDate = moment(dataRangeEnd).add(nUnits, unit);
  }

  console.log(
    `New params are: toDate ${moment(toDate).format(
      dateDisplayFormat
    )}, nUnits ${nUnits}`
  );
  return [toDate, nUnits];
}

function minMaxDates(data, fallbackDate) {
  if (data.length === 0) {
    return [fallbackDate, fallbackDate];
  }

  // data is supposed to be ordered oldest to newest (top entry is newest)
  const rangeStart = moment(data[data.length - 1].time * 1000);
  const rangeEnd = moment(data[0].time * 1000);

  return [rangeStart, rangeEnd];
}

function countUnits(startDate, endDate) {
  const diff = Math.max(endDate.diff(startDate, unit), 0);
  return Math.min(diff, maxUnitsInBatch);
}

function mergeData(d1, d2) {
  return uniqBy([...d1, ...d2], "time").sort(
    (a, b) =>
      // desc
      b.time - a.time
  );
}

async function fetchCCHourly(toDate, limit) {
  // server uses unix-style timestamps (without millis)
  const toTs = Math.floor(
    moment(toDate)
      .toDate()
      .getTime() / 1000
  );
  console.log(
    `Fetching ${sym1}-${sym2} data. ${limit} items until ${moment(
      toDate
    ).format(dateDisplayFormat)}`
  );

  const { data = {}, status } = await ccApi.get("/data/histohour", {
    params: {
      fsym: sym1,
      tsym: sym2,
      toTs,
      limit
    }
  });

  if (status == 200 && data.Response === "Success") {
    return data.Data;
  }

  throw new Error(`${status} - ${data.Response}`);
}

async function fetchCCHourlyMock(toDate, limit) {
  console.log(
    `*** MOCK *** Fetching ${sym1}-${sym2} data. ${limit} items until ${moment(
      toDate
    ).format(dateDisplayFormat)}`
  );

  return [
    // newest entry
    {
      time: Math.floor(
        moment(toDate)
          .toDate()
          .getTime() / 1000
      )
    },
    // eldest entry
    {
      time: Math.floor(
        moment(toDate)
          .subtract(limit, unit)
          .toDate()
          .getTime() / 1000
      )
    }
  ];
}

async function loadFile() {
  try {
    const file = await readFileP(`./${filename}`);
    const parsed = JSON.parse(file);
    console.log(`Loaded ${filename}`);
    return parsed;
  } catch (e) {
    console.log("No preexisting or corrupted file found");
    return [];
  }
}

async function saveFile(content) {
  content = JSON.stringify(content, null, 2);
  await writeFileP(`./${filename}`, content);
  console.log(`saved ${filename}`);
}

main().then(() => {
  console.log();
  console.log("----------- DONE -----------");
});
