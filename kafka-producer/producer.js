const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
	brokers: ['kafka:9092'],
	clientId: 'example-producer',
});

const topic = 'weather';
const producer = kafka.producer();

// const createMessage = (data) => {
// 	return { value: JSON.stringify(data) };
// };

const sendMessage = (data) => {
	return producer
		.send({
			topic,
			compression: CompressionTypes.GZIP,
			messages: [{ value: JSON.stringify(data) }],
		})
		.then(console.log)
		.catch((e) => console.error(`[example/producer] ${e.message}`, e));
};

const run = async (data) => {
	await producer.connect();
	console.log('Connected');
	sendMessage({
		tagId: '1234',
		channelId: '123',
		publisherId: '12',
		adsSourceId: '1',
		publisherChannelId: '56',
		connectionId: '12',
	});
};

setTimeout(() => {
	run().catch((e) => console.error(`[example/producer] ${e.message}`, e));
}, 30000);

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.map((type) => {
	process.on(type, async () => {
		try {
			console.log(`process.on ${type}`);
			await producer.disconnect();
			process.exit(0);
		} catch (_) {
			process.exit(1);
		}
	});
});

signalTraps.map((type) => {
	process.once(type, async () => {
		try {
			await producer.disconnect();
		} finally {
			process.kill(process.pid, type);
		}
	});
});

module.exports = { sendMessage };
