const express = require('express');
const cors = require('cors');
var cookieParser = require('cookie-parser');
const reportRouter = require('./routes/reportingRouter');

const app = express();
const port = 4005;

const corsOptions = {
	origin: true,
	credentials: true,
};
app.use(express.json());
app.use(cors(corsOptions)); // Allows cross origin communication
app.use(cookieParser()); // To parse JWTs

app.use('/reports', reportRouter);
app.listen(port, () => {
	console.log(`Server is running on port ${port}`);
});
