const express = require('express');
const router = express.Router();
const reportingController = require('../controllers/reportingController');

// router.get('/report', reportingController.getAllReport);
router.post('/report', reportingController.createReport);

module.exports = router;
