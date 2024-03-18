const { sendMessage } = require('../producer');

const createReport = async (req, res) => {
	console.log('createReport');
	try {
		const {
			tgid, //tagid
			pbid, //publisherid
			asid, //adssourceid
			pcid, //publisherchannelid
			pnid, //connectionid
			chid, //channelid
		} = req.query;

		sendMessage({
			tagId: tgid,
			channelId: chid,
			publisherId: pbid,
			adsSourceId: asid,
			publisherChannelId: pcid,
			connectionId: pnid,
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
