
const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
    brokers: ['kafka:9092'],
    clientId: 'example-producer',
});

const topic = 'weather';
const producer = kafka.producer();
let value = 0;
const logdata = [
    {
        "tagId": "1234",
        "channelId": "12",
        "publisherId": "12",
        "adsSourceId": "12",
        "publisherChannelId": "12",
        "connectionId": "12",
    },
    {
        "tagId": "123",
        "channelId": "12",
        "publisherId": "12",
        "adsSourceId": "12",
        "publisherChannelId": "12",
        "connectionId": "12",
    }
]

const createMessage = (data) => {
    // return JSON.stringify(data);
    return { value: JSON.stringify(data) }
}

const sendMessage = (i) => {
    const data = i % 2 === 0 ? logdata[0] : logdata[1];

    return producer
        .send({
            topic,
            compression: CompressionTypes.GZIP,
            // messages: [{ city: 'India', temperature: Math.random() }],
            // messages: [{ value: JSON.stringify(data) }],
            messages: Array(10000)
                .fill()
                .map(_ => createMessage(data)),
        })
        .then(console.log)
        .catch((e) => console.error(`[example/producer] ${e.message}`, e));
};

const run = async () => {
    await producer.connect();
    console.log('Connected');
    for (let i = 0; i < 10; i++) {
        setTimeout(() => {
            sendMessage(i);
        }, i * 1000);
    }
};

// setTimeout(() => {
run().catch((e) => console.error(`[example/producer] ${e.message}`, e));
// }, 30000);

// const errorTypes = ['unhandledRejection', 'uncaughtException'];
// const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

// errorTypes.map((type) => {
// 	process.on(type, async () => {
// 		try {
// 			console.log(`process.on ${type}`);
// 			await producer.disconnect();
// 			process.exit(0);
// 		} catch (_) {
// 			process.exit(1);
// 		}
// 	});
// });

// signalTraps.map((type) => {
// 	process.once(type, async () => {
// 		try {
// 			await producer.disconnect();
// 		} finally {
// 			process.kill(process.pid, type);
// 		}
// 	});
// });

// module.exports = { run };
