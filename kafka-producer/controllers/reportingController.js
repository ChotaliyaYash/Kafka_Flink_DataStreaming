const { sendMessage } = require('../producer');

const createReport = async (req, res) => {
	console.log('createReport');
	try {
		const {
			tagId,
			publisherId,
			adsSourceId,
			publisherChannelId,
			connectionId,
			channelId,
		} = req.query;

		sendMessage({
			tagId: '1234',
			channelId: '123',
			publisherId: '12',
			adsSourceId: '1',
			publisherChannelId: '56',
			connectionId: '12',
		});

		return res.status(201).json({
			message: 'Data recieved',
			success: true,
		});
	} catch (error) {
		console.error('Error creating Status:', error);
		res.status(500).json({
			message: 'Internal Server Error',
			success: false,
		});
	}
};

module.exports = {
	createReport,
};
