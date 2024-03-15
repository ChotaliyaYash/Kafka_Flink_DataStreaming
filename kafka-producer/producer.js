
const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
    brokers: ['kafka:9092'],
    clientId: 'example-producer',
});

const topic = 'weather';
const producer = kafka.producer();
let value = 0;
const sendMessage = () => {
    value += 1;
    const data = {
        city: 'India',
        temperature: value,
    }
    console.log(data);
    return producer
        .send({
            topic,
            compression: CompressionTypes.GZIP,
            // messages: [{ city: 'India', temperature: Math.random() }],
            messages: [{ value: JSON.stringify(data) }],
        })
        .then(console.log)
        .catch((e) => console.error(`[example/producer] ${e.message}`, e));
};

const run = async () => {
    await producer.connect();
    console.log('Connected');
    setInterval(() => {
        sendMessage();
    }, 10000);
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
