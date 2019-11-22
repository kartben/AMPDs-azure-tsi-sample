var fs = require('fs');
var parse = require('csv-parse');
const stream = require('stream');
const { default: PQueue } = require('p-queue');

const parser = parse({ delimiter: ',', columns: true, cast: true });

const { EventHubProducerClient } = require("@azure/event-hubs");
const config = require("config");
const connectionString = config.get("EventHubs.connectionString");
const eventHubName = config.get("EventHubs.eventHubName");

const queue = new PQueue();

class AggregatingTransform extends stream.Transform {
  constructor() {
    super({ objectMode: true });
    this.buffer = [];
  }

  _transform(record, encoding, callback) {
    //  console.log(record)
    this.buffer.push(record)
    if (this.buffer.length == 100) {
      callback(null, this.buffer);
      this.buffer = [];
    } else {
      callback(null);
    }
  }

  _flush(callback) {
    callback(null, this.buffer);
    this.buffer = []
  }
}
const aggregator = new AggregatingTransform();

totalMessagesSent = 0;

BATCH_SIZE = 500;
class EventHubPublisher extends stream.Writable {

  constructor() {
    super({ objectMode: true });
    this.eventHubProducer = new EventHubProducerClient(connectionString, eventHubName);
  }

  addMessage(m) {
    return this.eventDataBatch.tryAdd(m);
  }

  scheduleSendBatch(callback, flush) {
    if (flush || this.eventDataBatch.count == BATCH_SIZE) {
      queue.add(() => this.eventHubProducer.sendBatch(this.eventDataBatch)).then(() => {
        console.log(`${new Date()} -- send enqueued ${this.eventDataBatch.count} messages. Total ${totalMessagesSent += this.eventDataBatch.count}`)
        delete this.eventDataBatch;
        callback();
      }).catch(callback);
    } else {
      callback();
    }
  }

  _write(chunk, encoding, callback) {
    if (typeof this.eventDataBatch == "undefined") {
      this.eventHubProducer.createBatch().then((edb) => {
        this.eventDataBatch = edb;
        if (!this.addMessage(chunk)) {
          callback('Error adding message to batch');
        }
        this.scheduleSendBatch(callback);
      }).catch(callback)
    } else {
      if (!this.addMessage(chunk)) {
        callback('Error adding message to batch');
      }
      this.scheduleSendBatch(callback);
    }
  }

  _final(callback) {
    this.scheduleSendBatch(callback, true);
  }
}
const eventHubPublisher = new EventHubPublisher();

var debugStream = new stream.Transform({ objectMode: true });
debugStream._transform = function (chunk, _, done) {
  // console.log(chunk.length);
  //  console.log(chunk[0]);
  done(null, chunk);
};


for (i = 0; i < 40; i++) {
  stream.pipeline(
    fs.createReadStream(__dirname + '/Climate_HourlyWeather.csv'),
    parse({ delimiter: ',', columns: true, cast: true }),
    // new AggregatingTransform(),
    //  debugStream,
    new EventHubPublisher(),
    (err) => {
      if (err) {
        console.error('Pipeline failed', err);
      } else {
        console.log('Pipeline succeeded');
      }
    }
  );
  //    console.log(i)
}