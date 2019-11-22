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
    if (this.buffer.length == 200) {
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

class EventHubPublisher extends stream.Writable {

  constructor() {
    super({ objectMode: true });
    this.eventHubProducer = new EventHubProducerClient(connectionString, eventHubName);
  }

  _write(chunk, encoding, callback) {
    //    console.log(chunk.length);
    this.eventHubProducer.createBatch().then((eventDataBatch) => {
      let numberOfEventsToSend = chunk.length;
      while (numberOfEventsToSend > 0) {
        // console.log(chunk[chunk.length - numberOfEventsToSend])
        let wasAdded = eventDataBatch.tryAdd(chunk[chunk.length - numberOfEventsToSend]);
        if (!wasAdded) {
          break;
        }
        numberOfEventsToSend--;
      }

      queue.add(() => this.eventHubProducer.sendBatch(eventDataBatch)).then(() => {
        callback();
//        console.log(`${new Date()} -- send enqueued ${chunk.length} messages`)
      }).catch(callback);

      // eventHubProducer.sendBatch(eventDataBatch).then(() => {
      //   callback();
      //   console.log('sent ', chunk.length, ' elements')
      // }).catch(callback)
    }).catch(callback)
  }
}
const eventHubPublisher = new EventHubPublisher();

var debugStream = new stream.Transform({ objectMode: true });
debugStream._transform = function (chunk, _, done) {
  // console.log(chunk.length);
  //  console.log(chunk[0]);
  done(null, chunk);
};


for (i = 0; i < 1; i++) {
  stream.pipeline(
    fs.createReadStream(__dirname + '/Electricity_P.csv'),
    parse({ delimiter: ',', columns: true, cast: true }),
    new AggregatingTransform(),
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