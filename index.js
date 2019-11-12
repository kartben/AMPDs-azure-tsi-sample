const { EventHubProducerClient } = require("@azure/event-hubs");
const config = require("config");
const connectionString = config.get("EventHubs.connectionString");
const eventHubName = config.get("EventHubs.eventHubName");

const csv = require('csvtojson')
//const ProgressBar = require('progress');
const _cliProgress = require('cli-progress');

// Event Hubs send batch size
const BATCH_SIZE = 300;

const producer = new EventHubProducerClient(connectionString, eventHubName);

// create new container
const multibar = new _cliProgress.MultiBar({
    clearOnComplete: false,
    hideCursor: true,
    format: '{filename} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'
}, _cliProgress.Presets.shades_classic);

async function csv_send(filePath, timeSeriesID, timestampParser, bodyParser) {
    var csvArray = await csv({ checkType: true }).fromFile(filePath);

    var bar = multibar.create(csvArray.length, 0, {filename: filePath.padEnd(30)});

    batch = await producer.createBatch();
    for (let i = 0; i < csvArray.length; i++) {
        const element = csvArray[i];

        if (i > 0 && i % BATCH_SIZE == 0) {
            await producer.sendBatch(batch);
            bar.increment(BATCH_SIZE);
            batch = await producer.createBatch();
        } else {
            var m = { body: {}, properties: {} };
            m.body = bodyParser(element);
            m.properties['iothub-connection-device-id'] = timeSeriesID;
            m.properties['iothub-creation-time-utc'] = timestampParser(element);

            const isAdded = batch.tryAdd(m);
            if (!isAdded) {
                console.log(`Unable to add event ${i} to the batch`);
                break;
            }
        }
    }
    await producer.sendBatch(batch);
}


async function main() {
    csv_send('./Climate_HourlyWeather.csv',
        'XYZ',
        (e) => new Date(e['Date/Time']).toISOString(),
        (e) => e);

    csv_send('./Electricity_P.csv',
        'XYZ',
        (e) => new Date(e['UNIX_TS'] * 1000).toISOString(),
        (e) => e);

    csv_send('./NaturalGas_FRG.csv',
        'XYZ',
        (e) => new Date(e['unix_ts'] * 1000).toISOString(),
        (e) => { return { gas_furnace_avg: e.avg_rate, gas_furnace_instant: e.inst_rate } });

    csv_send('./NaturalGas_WHG.csv',
        'XYZ',
        (e) => new Date(e['unix_ts'] * 1000).toISOString(),
        (e) => { return { gas_wh_avg: e.avg_rate, gas_wh_instant: e.inst_rate } });

}

main().catch((err) => {
    console.log("Error occurred: ", err);
});

