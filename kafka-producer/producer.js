
const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
    brokers: ['kafka:9092'],
    clientId: 'example-producer',
});

const topic = 'weather';
const producer = kafka.producer();

const logdata = [
    {
        "tagId": "1234",
        "channelId": "123",
        "publisherId": "12",
        "adsSourceId": "1",
        "publisherChannelId": "56",
        "connectionId": "12",
    },
    {
        "tagId": "1234",
        "channelId": "1234",
        "publisherId": "12",
        "adsSourceId": "2",
        "publisherChannelId": "56",
        "connectionId": "12",
    },
    {
        "tagId": "123",
        "channelId": "123",
        "publisherId": "12",
        "adsSourceId": "1",
        "publisherChannelId": "44",
        "connectionId": "12",
    },
    {
        "tagId": "123",
        "channelId": "1234",
        "publisherId": "12",
        "adsSourceId": "2",
        "publisherChannelId": "33",
        "connectionId": "12",
    }

]

const createMessage = (data) => {
    // return JSON.stringify(data);
    return { value: JSON.stringify(data) }
}

const sendMessage = () => {
    value = Math.floor(Math.random() * 4);
    const data = logdata[value];

    return producer
        .send({
            topic,
            compression: CompressionTypes.GZIP,
            // messages: [{ city: 'India', temperature: Math.random() }],
            // messages: [{ value: JSON.stringify(data) }],
            messages: Array(Math.floor(Math.random() * 10000))
                .fill()
                .map(_ => createMessage(data)),
        })
        .then(console.log)
        .catch((e) => console.error(`[example/producer] ${e.message}`, e));
};

const run = async () => {
    await producer.connect();
    console.log('Connected');
    setInterval(() => {
        sendMessage();
    }, 100);
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
