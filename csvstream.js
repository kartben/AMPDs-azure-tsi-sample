var fs = require('fs');
var parse = require('csv-parse');
const stream = require('stream');
const { default: PQueue } = require('p-queue');
const _cliProgress = require('cli-progress');

const { EventHubProducerClient } = require("@azure/event-hubs");
const config = require("config");
const connectionString = config.get("EventHubs.connectionString");
const eventHubName = config.get("EventHubs.eventHubName");

const queue = new PQueue();

// create new container
const multibar = new _cliProgress.MultiBar({
  clearOnComplete: false,
  hideCursor: true,
  format: '{dataSetName} [{bar}] {percentage}% | ETA: {eta_formatted} | {value}/{total} - Running time: {duration_formatted}'
}, _cliProgress.Presets.shades_classic);

BATCH_SIZE = 200;
class EventHubPublisher extends stream.Writable {

  constructor(dataSetName, numberOfRows) {
    super({ objectMode: true });
    this.eventHubProducer = new EventHubProducerClient(connectionString, eventHubName);
    this.bar = multibar.create(numberOfRows, 0, { dataSetName: dataSetName.padEnd(30) });
  }

  addMessage(m, callback) {
    delete m.row;
//    console.log(m)
    if (!this.eventDataBatch.tryAdd(m)) {
      callback('Error adding message to batch');
    }
    this.scheduleSendBatch(callback);
  }

  scheduleSendBatch(callback, flush) {
    if (flush || this.eventDataBatch.count == BATCH_SIZE) {
      queue.add(() => this.eventHubProducer.sendBatch(this.eventDataBatch)).then(() => {
        //        console.log(`Send enqueued ${this.eventDataBatch.count} messages. Total ${totalMessagesSent += this.eventDataBatch.count}`)
        this.bar.increment(this.eventDataBatch.count);
        delete this.eventDataBatch;
        callback();
      }).catch(callback);
    } else {
      callback();
    }
  }

  _write(chunk, encoding, callback) {
    //console.log(JSON.stringify(chunk));
    //console.log(chunk)
    if (typeof this.eventDataBatch == "undefined") {
      this.eventHubProducer.createBatch().then((edb) => {
        this.eventDataBatch = edb;
        this.addMessage(chunk, callback);
      }).catch(callback)
    } else {
      this.addMessage(chunk, callback);
    }
  }

  _final(callback) {
    this.scheduleSendBatch(callback, true);
    this.bar.stop();
  }
}

class TimeStampParser extends stream.Transform {
  constructor(fn) {
    super({ objectMode: true });
    this.fn = fn;
  }
  _transform(chunk, _, done) {
    chunk.properties['iothub-creation-time-utc'] = this.fn(chunk.row)
    done(null, chunk);
  }
}

class FieldEraser extends stream.Transform {
  constructor(fieldsToErase) {
    super({ objectMode: true });
    this.fieldsToErase = fieldsToErase;
  }
  _transform(chunk, _, done) {
    this.fieldsToErase.forEach(f => {
      delete chunk.f;
    });
    done(null, chunk);
  }
}


function streamCsvToEventHub(timeseriesName, csvPath, numberOfRows, tsExtractFunction) {
  stream.pipeline(
    fs.createReadStream(__dirname + csvPath),
    parse({ delimiter: ',', columns: true, cast: true }),
    new stream.Transform({ objectMode: true, transform: (chunk, _, cb) => cb(null, { row: chunk, properties: {'iothub-connection-device-id': timeseriesName} }) }),
    new TimeStampParser(tsExtractFunction),
    new stream.Transform({objectMode: true, transform: (chunk,_,cb) => cb(null, { ...chunk, body: chunk.row })}),
    // new AggregatingTransform(),
    //  debugStream,
    new EventHubPublisher(timeseriesName, numberOfRows),
    (err) => {
      if (err) {
        console.error('Pipeline failed', err);
      } else {
        console.log('Pipeline succeeded');
      }
    }
  );

}

streamCsvToEventHub('Weather004', '/Climate_HourlyWeather.csv', 17_520, (e) => new Date(e['Date/Time']).toISOString())
streamCsvToEventHub('Electricity004', '/Electricity_P.csv', 1_051_200, (e) => new Date(e['UNIX_TS'] * 1000).toISOString())

// csv_send('./NaturalGas_FRG.csv',
// 'XYZ',
// (e) => new Date(e['unix_ts'] * 1000).toISOString(),
// (e) => { return { gas_furnace_avg: e.avg_rate, gas_furnace_instant: e.inst_rate } });

// csv_send('./NaturalGas_WHG.csv',
// 'XYZ',
// (e) => new Date(e['unix_ts'] * 1000).toISOString(),
// (e) => { return { gas_wh_avg: e.avg_rate, gas_wh_instant: e.inst_rate } });
